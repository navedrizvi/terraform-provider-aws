// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package dynamodb

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
	"github.com/hashicorp/terraform-provider-aws/internal/conns"
	"github.com/hashicorp/terraform-provider-aws/internal/create"
	"github.com/hashicorp/terraform-provider-aws/internal/flex"
	"github.com/hashicorp/terraform-provider-aws/names"
)

// @SDKDataSource("aws_dynamodb_table_query")
func DataSourceTableQuery() *schema.Resource {
	return &schema.Resource{
		ReadWithoutTimeout: dataSourceTableQueryRead,

		Schema: map[string]*schema.Schema{
			"table_name": {
				Type:     schema.TypeString,
				Required: true,
			},
			"key_condition_expression": {
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
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
			// TODO1 -  Think more about if this would be more user friendly to accept block or map instead of an escaped JSON string. The API just takes a string. But we need to escape any quotes...This way passes the JSON as a string with all the quotes escaped:
			// # (continued from above)
			// #
			// # Heredoc syntax might let us remove the quote escaping (I think?)
			// # exclusive_start_key = <<-EOT
			// #   {"S": "example-hash-key-12345"}
			// #   EOT
			// #
			// # This does more magic than passing the start key as just a string, but may
			// # be a cleaner interface if designed right. I'm not sure what that right way
			// # is. Leaning towards just using a string to keep it simple. But doing it
			// # this way might enable us to do intelligent type checking before sending
			// # queries.
			// #
			// # exclusive_start_key {
			// #   type   = "S"
			// #   values = ["example-hash-key-12345"]
			// # }
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
			"limit": {
				Type:     schema.TypeInt,
				Optional: true,
			},
			// TODO0 - test
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
			"item_count": {
				Type:     schema.TypeInt,
				Computed: true,
			},
		},
	}
}
func dataSourceTableQueryRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	conn := meta.(*conns.AWSClient).DynamoDBConn(ctx)

	tableName := d.Get("table_name").(string)
	keyConditionExpression := d.Get("key_condition_expression").(string)
	consistentRead := d.Get("consistent_read").(bool)
	scanIndexForward := d.Get("scan_index_forward").(bool)

	in := &dynamodb.QueryInput{
		TableName:        aws.String(tableName),
		ConsistentRead:   aws.Bool(consistentRead),
		ScanIndexForward: aws.Bool(scanIndexForward),
	}

	filterExpression := d.Get("filter_expression").(string)
	indexName := d.Get("index_name").(string)
	limit := int64(d.Get("limit").(int))
	projectionExpression := d.Get("projection_expression").(string)
	returnConsumedCapacity := d.Get("return_consumed_capacity").(string)
	_select := d.Get("select").(string)

	if v, ok := d.GetOk("expression_attribute_names"); ok && len(v.(map[string]interface{})) > 0 {
		in.ExpressionAttributeNames = flex.ExpandStringMap(v.(map[string]interface{}))
	}

	if v, ok := d.GetOk("expression_attribute_values"); ok && len(v.(map[string]interface{})) > 0 {
		expressionAttributeValues, err := dynamodbattribute.MarshalMap(v)
		if err != nil {
			return diag.FromErr(err)
		}
		if expressionAttributeValues != nil && len(expressionAttributeValues) > 0 {
			in.ExpressionAttributeValues = expressionAttributeValues
		}
		log.Println("[ERROR] eAV:", expressionAttributeValues)
	}

	exclusiveStartKey, _ := ExpandTableItemAttributes(d.Get("exclusive_start_key").(string))
	if exclusiveStartKey != nil && len(exclusiveStartKey) > 0 {
		in.ExclusiveStartKey = exclusiveStartKey
	}

	if filterExpression != "" {
		in.FilterExpression = aws.String(filterExpression)
	}

	if indexName != "" {
		in.IndexName = aws.String(indexName)
	}

	if keyConditionExpression != "" {
		in.KeyConditionExpression = aws.String(keyConditionExpression)
	}

	if limit > 0 {
		in.Limit = aws.Int64(limit)
	}

	if projectionExpression != "" {
		in.ProjectionExpression = aws.String(projectionExpression)
	}

	if returnConsumedCapacity != "" {
		in.ReturnConsumedCapacity = aws.String(returnConsumedCapacity)
	}

	if _select != "" {
		in.Select = aws.String(_select)
	}

	out, err := conn.QueryWithContext(ctx, in)
	if err != nil {
		return diag.FromErr(err)
	}

	log.Println("[ERROR] oooout:", out)

	id := buildTableQueryDataSourceID(tableName, indexName, keyConditionExpression)
	d.SetId(id)

	var flattenedItems []string
	for _, item := range out.Items {
		flattened, err := flattenTableItemAttributes(item)
		if err != nil {
			return create.DiagError(names.DynamoDB, create.ErrActionReading, DSNameTableItem, id, err)
		}
		flattenedItems = append(flattenedItems, flattened)
	}

	d.Set("last_evaluated_key", out.LastEvaluatedKey)
	d.Set("scanned_count", out.ScannedCount)
	d.Set("consumed_capacity", out.ConsumedCapacity)
	d.Set("items", flattenedItems)
	// count is a reserved field name, so use item_count
	d.Set("item_count", out.Count)

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
