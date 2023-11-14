// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package dynamodb

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
	"github.com/hashicorp/terraform-provider-aws/internal/conns"
	"github.com/hashicorp/terraform-provider-aws/internal/create"
	"github.com/hashicorp/terraform-provider-aws/names"
)

// @SDKDataSource("aws_dynamodb_table_query")
func DataSourceTableQuery() *schema.Resource {
	return &schema.Resource{
		ReadContext: dataSourceTableQueryRead,
		Schema: map[string]*schema.Schema{
			"table_name": {
				Type:     schema.TypeString,
				Required: true,
			},
			"consistent_read": {
				Type:     schema.TypeBool,
				Optional: true,
				Default:  false,
			},
			"items": {
				Type:     schema.TypeList,
				Computed: true,
				Elem:     schema.TypeString,
			},
			"exclusive_start_key": {
				Type:         schema.TypeString,
				Optional:     true,
				ValidateFunc: validateTableItem,
			},
			"expression_attribute_names": {
				Type:     schema.TypeMap,
				Optional: true,
				Elem:     &schema.Schema{Type: schema.TypeString},
			},
			"expression_attribute_values": {
				Type:     schema.TypeMap,
				Optional: true,
				Elem:     &schema.Schema{Type: schema.TypeString},
			},
			"filter_expression": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"index_name": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"key_condition_expression": {
				Type:     schema.TypeString,
				Required: false,
			},
			"limit": {
				Type:     schema.TypeInt,
				Optional: true,
			},
			"projection_expression": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"return_consumed_capacity": {
				Type:         schema.TypeString,
				Optional:     true,
				Default:      "NONE",
				ValidateFunc: validation.StringInSlice([]string{"NONE", "INDEXES", "TOTAL"}, false),
			},
			"scan_index_forward": {
				Type:     schema.TypeBool,
				Optional: true,
				Default:  true,
			},
			"select": {
				Type:         schema.TypeString,
				Optional:     true,
				Default:      "ALL_ATTRIBUTES",
				ValidateFunc: validation.StringInSlice([]string{"ALL_ATTRIBUTES", "ALL_PROJECTED_ATTRIBUTES", "SPECIFIC_ATTRIBUTES", "COUNT"}, false),
			},
			"last_evaluated_key": {
				Type:     schema.TypeMap,
				Computed: true,
				Elem:     &schema.Schema{Type: schema.TypeString},
			},
			"scanned_count": {
				Type:     schema.TypeInt,
				Computed: true,
			},
			"consumed_capacity": {
				Type:     schema.TypeMap,
				Computed: true,
				Elem:     &schema.Schema{Type: schema.TypeString},
			},
			"count": {
				Type:     schema.TypeInt,
				Computed: true,
			},
		},
	}
}

func dataSourceTableQueryRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	conn := meta.(*conns.AWSClient).DynamoDBConn(ctx)

	tableName := d.Get("table_name").(string)
	consistentRead := d.Get("consistent_read").(bool)

	exclusiveStartKey, err := ExpandTableItemAttributes(d.Get("exclusive_start_key").(string))
	if err != nil {
		return diag.FromErr(err)
	}

	var expressionAttributeNames map[string]*string
	if v, ok := d.GetOk("expression_attribute_names"); ok && len(v.(map[string]interface{})) > 0 {
		expressionAttributeNames = make(map[string]*string)
		for key, val := range v.(map[string]interface{}) {
			strVal := val.(string)
			expressionAttributeNames[key] = &strVal
		}
	}

	var expressionAttributeValues map[string]*dynamodb.AttributeValue
	if v, ok := d.GetOk("expression_attribute_values"); ok && len(v.(map[string]interface{})) > 0 {
		expressionAttributeValues = make(map[string]*dynamodb.AttributeValue)
		for key, val := range v.(map[string]interface{}) {
			strVal := val.(string)
			expressionAttributeValues[key] = &dynamodb.AttributeValue{S: aws.String(strVal)}
		}
	}

	filterExpression := d.Get("filter_expression").(string)
	indexName := d.Get("index_name").(string)
	keyConditionExpression := d.Get("key_condition_expression").(string)
	limit := int64(d.Get("limit").(int))
	projectionExpression := d.Get("projection_expression").(string)

	returnConsumedCapacity := d.Get("return_consumed_capacity").(string)
	scanIndexForward := d.Get("scan_index_forward").(bool)
	_select := d.Get("select").(string)

	id := buildTableQueryDataSourceID(tableName, indexName, keyConditionExpression)
	d.SetId(id)

	queryInput := &dynamodb.QueryInput{
		TableName:                 aws.String(tableName),
		ConsistentRead:            aws.Bool(consistentRead),
		ExclusiveStartKey:         exclusiveStartKey,
		ExpressionAttributeNames:  expressionAttributeNames,
		ExpressionAttributeValues: expressionAttributeValues,
		FilterExpression:          aws.String(filterExpression),
		IndexName:                 aws.String(indexName),
		KeyConditionExpression:    aws.String(keyConditionExpression),
		Limit:                     aws.Int64(limit),
		ProjectionExpression:      aws.String(projectionExpression),
		ReturnConsumedCapacity:    aws.String(returnConsumedCapacity),
		ScanIndexForward:          aws.Bool(scanIndexForward),
		Select:                    aws.String(_select),
	}

	out, err := conn.QueryWithContext(ctx, queryInput)
	if err != nil {
		return diag.FromErr(err)
	}

	items := make([]string, 0, len(out.Items))
	for _, item := range out.Items {
		converted, err := flattenTableItemAttributes(item)
		if err != nil {
			return create.DiagError(names.DynamoDB, create.ErrActionReading, DSNameTableItem, id, err)
		}
		items = append(items, converted)
	}

	d.Set("items", items)
	d.Set("last_evaluated_key", out.LastEvaluatedKey)
	d.Set("scanned_count", out.ScannedCount)
	// TODO1 - test
	d.Set("consumed_capacity", out.ConsumedCapacity)
	d.Set("count", out.Count)

	return nil
}

func buildTableQueryDataSourceID(tableName string, indexName string, keyConditionExpression string) string {
	id := []string{tableName}

	if keyConditionExpression != "" {
		id = append(id, "KeyConditionExpression", keyConditionExpression)
	}

	if indexName != "" {
		id = append(id, "IndexName", indexName)
	}

	id = append(id, fmt.Sprintf("%d", time.Now().UnixNano()))

	return strings.Join(id, "|")
}
