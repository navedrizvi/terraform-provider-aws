// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package dynamodb

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
	"github.com/hashicorp/terraform-provider-aws/internal/conns"
	"github.com/hashicorp/terraform-provider-aws/internal/create"
	"github.com/hashicorp/terraform-provider-aws/internal/flex"
	"github.com/hashicorp/terraform-provider-aws/names"
)

type AttributeValue struct {
	B    []byte                     `json:"B,omitempty"`
	BOOL *bool                      `json:"BOOL,omitempty"`
	BS   [][]byte                   `json:"BS,omitempty"`
	L    []*AttributeValue          `json:"L,omitempty"`
	M    map[string]*AttributeValue `json:"M,omitempty"`
	N    *string                    `json:"N,omitempty"`
	NS   []*string                  `json:"NS,omitempty"`
	NULL *bool                      `json:"NULL,omitempty"`
	S    *string                    `json:"S,omitempty"`
	SS   []*string                  `json:"SS,omitempty"`
}

func ConvertJSONToAttributeValue(jsonStr string) (*AttributeValue, error) {
	data := AttributeValue{}
	unescapedJSONStr, err := strconv.Unquote(jsonStr)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal([]byte(unescapedJSONStr), &data)
	if err != nil {
		return nil, err
	}
	return &data, nil
}

// TODO01 - unit test all attributevalues : Unit test
func ConvertToDynamoAttributeValue(av *AttributeValue) (*dynamodb.AttributeValue, error) {
	if av == nil {
		return nil, nil
	}
	dynamoAV := &dynamodb.AttributeValue{}
	if av.B != nil {
		dynamoAV.B = av.B
	}

	if av.BOOL != nil {
		dynamoAV.BOOL = av.BOOL
	}

	if av.BS != nil {
		var bs [][]byte
		for _, item := range av.BS {
			bs = append(bs, item)
		}
		dynamoAV.BS = bs
	}

	if av.L != nil {
		var l []*dynamodb.AttributeValue
		for _, item := range av.L {
			dynamoItem, err := ConvertToDynamoAttributeValue(item)
			if err != nil {
				return nil, err
			}
			l = append(l, dynamoItem)
		}
		dynamoAV.L = l
	}

	if av.M != nil {
		m := make(map[string]*dynamodb.AttributeValue)
		for k, v := range av.M {
			dynamoItem, err := ConvertToDynamoAttributeValue(v)
			if err != nil {
				return nil, err
			}
			m[k] = dynamoItem
		}
		dynamoAV.M = m
	}

	if av.N != nil {
		dynamoAV.N = av.N
	}

	if av.NS != nil {
		var ns []*string
		for _, item := range av.NS {
			ns = append(ns, item)
		}
		dynamoAV.NS = ns
	}

	if av.NULL != nil {
		dynamoAV.NULL = av.NULL
	}

	if av.S != nil {
		dynamoAV.S = av.S
	}

	if av.SS != nil {
		var ss []*string
		for _, item := range av.SS {
			ss = append(ss, item)
		}
		dynamoAV.SS = ss
	}

	return dynamoAV, nil
}

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
			// TODO012 - test
			"index_name": {
				Type:     schema.TypeString,
				Optional: true,
			},
			// // TODO1 comment that this page-size limit is not being used. we use a global limit. this one is used for pagination
			// handled by other -- Upto N results - repurposed
			// TODO012 - test Pagination
			"output_limit": {
				Type:     schema.TypeInt,
				Optional: true,
			},
			"projection_expression": {
				Type:     schema.TypeString,
				Optional: true,
			},
			// TODO0 - test
			"scan_index_forward": {
				Type:     schema.TypeBool,
				Optional: true,
				Default:  true,
			},
			"select": {
				Type:         schema.TypeString,
				Optional:     true,
				ValidateFunc: validation.StringInSlice([]string{"ALL_ATTRIBUTES", "ALL_PROJECTED_ATTRIBUTES", "SPECIFIC_ATTRIBUTES", "COUNT"}, false),
			},
			"scanned_count": {
				Type:     schema.TypeInt,
				Computed: true,
			},
			"item_count": {
				Type:     schema.TypeInt,
				Computed: true,
			},
			// TODO01 - test handling pagination...
			"query_count": {
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

	outputLimit := int64(d.Get("output_limit").(int))
	projectionExpression := d.Get("projection_expression").(string)
	_select := d.Get("select").(string)

	if v, ok := d.GetOk("expression_attribute_names"); ok && len(v.(map[string]interface{})) > 0 {
		in.ExpressionAttributeNames = flex.ExpandStringMap(v.(map[string]interface{}))
	}

	if v, ok := d.GetOk("expression_attribute_values"); ok && len(v.(map[string]interface{})) > 0 {
		expressionAttributeValues := flex.ExpandStringMap(v.(map[string]interface{}))
		attributeValues := make(map[string]*dynamodb.AttributeValue)
		for key, value := range expressionAttributeValues {
			jsonData, err := json.Marshal(value)
			if err != nil {
				return diag.FromErr(err)
			}
			attributeValue, err := ConvertJSONToAttributeValue(string(jsonData))
			if err != nil {
				return diag.FromErr(err)
			}
			dynamoAttributeValue, err := ConvertToDynamoAttributeValue(attributeValue)
			if err != nil {
				return diag.FromErr(err)
			}
			attributeValues[key] = dynamoAttributeValue
		}
		for key, value := range attributeValues {
			fmt.Printf("safdsafds %s: %#v\n", key, value)
		}
		in.ExpressionAttributeValues = attributeValues
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

	if projectionExpression != "" {
		in.ProjectionExpression = aws.String(projectionExpression)
	}

	if _select != "" {
		in.Select = aws.String(_select)
	}

	var flattenedItems []string
	itemsProcessed := int64(0)
	scannedCount := int64(0)
	queryCount := int64(0)
	itemCount := int64(0)
	id := buildTableQueryDataSourceID(tableName, indexName, keyConditionExpression)
	d.SetId(id)

	for {
		out, err := conn.QueryWithContext(ctx, in)
		fmt.Printf("[ERROR]2 out: %v\n", out.Items)
		fmt.Printf("[ERROR]2 out: %v\n", out.Count)
		fmt.Printf("[ERROR]2 out: %v\n", out.ScannedCount)

		queryCount += 1
		if err != nil {
			return diag.FromErr(err)
		}

		scannedCount += aws.Int64Value(out.ScannedCount)
		itemCount += aws.Int64Value(out.Count)
		for _, item := range out.Items {
			fmt.Printf("[ERROR]2 oiiii: %v\n", item)
			flattened, err := flattenTableItemAttributes(item)
			if err != nil {
				return create.DiagError(names.DynamoDB, create.ErrActionReading, DSNameTableItem, id, err)
			}
			flattenedItems = append(flattenedItems, flattened)

			itemsProcessed++
			if itemsProcessed >= outputLimit {
				goto ExitLoop
			}
		}
		in.ExclusiveStartKey = out.LastEvaluatedKey

		if out.LastEvaluatedKey == nil || len(out.LastEvaluatedKey) == 0 {
			break
		}
	}
ExitLoop:
	d.Set("items", flattenedItems)
	d.Set("item_count", itemCount)
	d.Set("query_count", queryCount)
	d.Set("scanned_count", scannedCount)
	fmt.Printf("[ERROR]2 items: %v\n", flattenedItems)
	fmt.Printf("[ERROR]2 item_count: %v\n", itemCount)
	fmt.Printf("[ERROR]2 query_count: %v\n", queryCount)
	fmt.Printf("[ERROR]2 scanned_count: %v\n", scannedCount)
	return nil
}

func buildTableQueryDataSourceID(tableName, indexName, keyConditionExpression string) string {
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
