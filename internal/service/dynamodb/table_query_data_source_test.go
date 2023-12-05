// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package dynamodb_test

import (
	"fmt"
	"log"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	sdkacctest "github.com/hashicorp/terraform-plugin-testing/helper/acctest"
	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-provider-aws/internal/acctest"
	tfdynamo "github.com/hashicorp/terraform-provider-aws/internal/service/dynamodb"
)

func TestAccDynamoDBTableQueryDataSource_basic(t *testing.T) {
	ctx := acctest.Context(t)
	rName := sdkacctest.RandomWithPrefix(acctest.ResourcePrefix)
	dataSourceName := "data.aws_dynamodb_table_query.test"
	hashKey := "hashKey"
	itemContent := `{
		"hashKey": {"S": "something"},
		"one": {"N": "11111"},
		"two": {"N": "22222"}
	}`

	resource.ParallelTest(t, resource.TestCase{
		PreCheck: func() {
			acctest.PreCheck(ctx, t)
			acctest.PreCheckPartitionHasService(t, dynamodb.EndpointsID)
		},
		ErrorCheck:               acctest.ErrorCheck(t, dynamodb.EndpointsID),
		ProtoV5ProviderFactories: acctest.ProtoV5ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: testAccTableQueryDataSourceConfig_basic(rName, itemContent, hashKey),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(dataSourceName, "items.#", "1"),
					acctest.CheckResourceAttrEquivalentJSON(dataSourceName, "items.0", itemContent),
					resource.TestCheckResourceAttr(dataSourceName, "table_name", rName),
					resource.TestCheckResourceAttr(dataSourceName, "scanned_count", "1"),
					resource.TestCheckResourceAttr(dataSourceName, "item_count", "1"),
					resource.TestCheckResourceAttr(dataSourceName, "query_count", "1"),
				),
			},
		},
	})
}

func TestAccDynamoDBTableQueryDataSource_projectionExpression(t *testing.T) {
	ctx := acctest.Context(t)
	rName := sdkacctest.RandomWithPrefix(acctest.ResourcePrefix)
	dataSourceName := "data.aws_dynamodb_table_query.test"
	hashKey := "hashKey"
	projectionExpression := "one,two"
	itemContent := `{
		"hashKey": {"S": "something"},
		"one": {"N": "11111"},
		"two": {"N": "22222"},
		"three": {"N": "33333"},
		"four": {"N": "44444"}
	}`

	expected := `{
		"one": {"N": "11111"},
		"two": {"N": "22222"}
	}`

	resource.ParallelTest(t, resource.TestCase{
		PreCheck: func() {
			acctest.PreCheck(ctx, t)
			acctest.PreCheckPartitionHasService(t, dynamodb.EndpointsID)
		},
		ErrorCheck:               acctest.ErrorCheck(t, dynamodb.EndpointsID),
		ProtoV5ProviderFactories: acctest.ProtoV5ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: testAccTableQueryDataSourceConfig_projectionExpression(rName, itemContent, projectionExpression, hashKey),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(dataSourceName, "items.#", "1"),
					acctest.CheckResourceAttrEquivalentJSON(dataSourceName, "items.0", expected),
					resource.TestCheckResourceAttr(dataSourceName, "table_name", rName),
				),
			},
		},
	})
}

func TestAccDynamoDBTableQueryDataSource_expressionAttributeNames(t *testing.T) {
	ctx := acctest.Context(t)
	rName := sdkacctest.RandomWithPrefix(acctest.ResourcePrefix)
	dataSourceName := "data.aws_dynamodb_table_query.test"
	hashKey := "hashKey"
	itemContent := `{
		"hashKey": {"S": "something"},
		"one": {"N": "11111"},
		"two": {"N": "22222"},
		"Percentile": {"N": "33333"}
	}`

	expected := `{
		"Percentile": {"N": "33333"}
	}`

	resource.ParallelTest(t, resource.TestCase{
		PreCheck: func() {
			acctest.PreCheck(ctx, t)
			acctest.PreCheckPartitionHasService(t, dynamodb.EndpointsID)
		},
		ErrorCheck:               acctest.ErrorCheck(t, dynamodb.EndpointsID),
		ProtoV5ProviderFactories: acctest.ProtoV5ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: testAccTableQueryDataSourceConfig_expressionAttributeNames(rName, itemContent, hashKey),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(dataSourceName, "items.#", "1"),
					acctest.CheckResourceAttrEquivalentJSON(dataSourceName, "items.0", expected),
					resource.TestCheckResourceAttr(dataSourceName, "table_name", rName),
					resource.TestCheckResourceAttr(dataSourceName, "projection_expression", "#P"),
				),
			},
		},
	})
}

func TestAccDynamoDBTableQueryDataSource_select(t *testing.T) {
	ctx := acctest.Context(t)
	rName := sdkacctest.RandomWithPrefix(acctest.ResourcePrefix)
	dataSourceName := "data.aws_dynamodb_table_query.test"
	hashKey := "hashKey"
	selectValue := "COUNT"
	itemContent := `{
		"hashKey": {"S": "something"},
		"one": {"N": "11111"},
		"two": {"N": "22222"},
		"three": {"N": "33333"},
		"four": {"N": "44444"}
	}`

	resource.ParallelTest(t, resource.TestCase{
		PreCheck: func() {
			acctest.PreCheck(ctx, t)
			acctest.PreCheckPartitionHasService(t, dynamodb.EndpointsID)
		},
		ErrorCheck:               acctest.ErrorCheck(t, dynamodb.EndpointsID),
		ProtoV5ProviderFactories: acctest.ProtoV5ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: testAccTableQueryDataSourceConfig_select(rName, itemContent, hashKey, selectValue),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(dataSourceName, "items.#", "0"),
					resource.TestCheckResourceAttr(dataSourceName, "item_count", "1"),
					resource.TestCheckResourceAttr(dataSourceName, "table_name", rName),
				),
			},
		},
	})
}

func TestAccDynamoDBTableQueryDataSource_filterExpression(t *testing.T) {
	ctx := acctest.Context(t)
	rName := sdkacctest.RandomWithPrefix(acctest.ResourcePrefix)
	dataSourceName := "data.aws_dynamodb_table_query.test"
	item1 := `{"ID": {"S": "something"}, "Category": {"S": "A"}}`
	item2 := `{"ID": {"S": "another"}, "Category": {"S": "B"}}`
	filterExpression := "Category = :category"

	expected := `{
  "ID": {"S": "something"},
  "Category": {"S": "A"}
}`

	resource.ParallelTest(t, resource.TestCase{
		PreCheck: func() {
			acctest.PreCheck(ctx, t)
		},
		ErrorCheck:               acctest.ErrorCheck(t, dynamodb.EndpointsID),
		ProtoV5ProviderFactories: acctest.ProtoV5ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: testAccTableQueryDataSourceConfig_filterExpression(rName, item1, item2, filterExpression),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(dataSourceName, "items.#", "1"),
					acctest.CheckResourceAttrEquivalentJSON(dataSourceName, "items.0", expected),
				),
			},
		},
	})
}

func TestAccDynamoDBTableQueryDataSource_scanIndexForward(t *testing.T) {
	ctx := acctest.Context(t)
	rName := sdkacctest.RandomWithPrefix(acctest.ResourcePrefix)
	dataSourceName := "data.aws_dynamodb_table_query.test"
	hashKey := "hashKey"
	sortKey := "sortKey"
	sortKeyValue1 := "sortValue1"
	sortKeyValue2 := "sortValue2"
	sortKeyValue3 := "sortValue3"
	scanIndexForward := false
	expected := []string{
		`{"hashKey":{"S":"something"},"three":{"N":"33333"},"sortKey":{"S":"sortValue3"}}`,
		`{"hashKey":{"S":"something"},"two":{"N":"22222"},"sortKey":{"S":"sortValue2"}}`,
		`{"hashKey":{"S":"something"},"one":{"N":"11111"},"sortKey":{"S":"sortValue1"}}`,
	}

	resource.ParallelTest(t, resource.TestCase{
		PreCheck: func() {
			acctest.PreCheck(ctx, t)
			acctest.PreCheckPartitionHasService(t, dynamodb.EndpointsID)
		},
		ErrorCheck:               acctest.ErrorCheck(t, dynamodb.EndpointsID),
		ProtoV5ProviderFactories: acctest.ProtoV5ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: testAccTableQueryDataSourceConfig_scanIndexForward(rName, hashKey, sortKey, sortKeyValue1, sortKeyValue2, sortKeyValue3, scanIndexForward),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(dataSourceName, "items.#", "3"),
					acctest.CheckResourceAttrEquivalentJSON(dataSourceName, "items.0", expected[0]),
					acctest.CheckResourceAttrEquivalentJSON(dataSourceName, "items.1", expected[1]),
					acctest.CheckResourceAttrEquivalentJSON(dataSourceName, "items.2", expected[2]),
					resource.TestCheckResourceAttr(dataSourceName, "table_name", rName),
				),
			},
		},
	})
}

func TestAccDynamoDBTableQueryDataSource_index(t *testing.T) {
	rName := sdkacctest.RandomWithPrefix(acctest.ResourcePrefix)
	dataSourceName := "data.aws_dynamodb_table_query.test"
	ctx := acctest.Context(t)

	indexName := "exampleIndex"
	itemContent := `{
		"hashKey": {"S": "something"},
		"value": {"N": "1111"},
		"extraAttribute": {"S": "additionalValue"}
	}`

	projectionType := "INCLUDE"
	expected := []string{
		`{"hashKey":{"S":"something"},"extraAttribute":{"S":"additionalValue"}}`,
	}

	resource.ParallelTest(t, resource.TestCase{
		PreCheck: func() {
			acctest.PreCheck(ctx, t)
			acctest.PreCheckPartitionHasService(t, dynamodb.EndpointsID)
		},
		ErrorCheck:               acctest.ErrorCheck(t, dynamodb.EndpointsID),
		ProtoV5ProviderFactories: acctest.ProtoV5ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: testAccTableQueryDataSourceConfig_index(rName, itemContent, projectionType, indexName),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(dataSourceName, "items.#", "1"),
					acctest.CheckResourceAttrEquivalentJSON(dataSourceName, "items.0", expected[0]),
					resource.TestCheckResourceAttr(dataSourceName, "table_name", rName),
				),
			},
		},
	})
}

func testAccTableQueryDataSourceConfig_basic(tableName, item, hashKey string) string {
	return fmt.Sprintf(`
resource "aws_dynamodb_table" "test" {
  name           = %[1]q
  read_capacity  = 10
  write_capacity = 10
  hash_key       = %[2]q

  attribute {
    name = %[3]q
    type = "S"
  }
}

resource "aws_dynamodb_table_item" "test" {
  table_name = aws_dynamodb_table.test.name
  hash_key   = aws_dynamodb_table.test.hash_key
  item = <<ITEM
%[4]s
ITEM
}

data "aws_dynamodb_table_query" "test" {
  table_name                  = aws_dynamodb_table.test.name
	key_condition_expression    = "hashKey = :value"
	expression_attribute_values = {
		":value"= jsonencode({"S" = "something"})
	}
  depends_on                  = [aws_dynamodb_table_item.test]
}
`, tableName, hashKey, hashKey, item)
}

func testAccTableQueryDataSourceConfig_projectionExpression(tableName, item, projectionExpression, hashKey string) string {
	return fmt.Sprintf(`
resource "aws_dynamodb_table" "test" {
  name           = %[1]q
  read_capacity  = 10
  write_capacity = 10
  hash_key       = %[2]q

  attribute {
    name = %[3]q
    type = "S"
  }
}

resource "aws_dynamodb_table_item" "test" {
  table_name = aws_dynamodb_table.test.name
  hash_key   = aws_dynamodb_table.test.hash_key
  item = <<ITEM
%[4]s
ITEM
}

data "aws_dynamodb_table_query" "test" {
  projection_expression = %[5]q
	select = "SPECIFIC_ATTRIBUTES"
  table_name                  = aws_dynamodb_table.test.name
	key_condition_expression    = "hashKey = :value"
	expression_attribute_values = {
		":value"= "{\"S\": \"something\"}"
	}
  depends_on                  = [aws_dynamodb_table_item.test]
}
`, tableName, hashKey, hashKey, item, projectionExpression)
}

func testAccTableQueryDataSourceConfig_expressionAttributeNames(tableName, item, hashKey string) string {
	return fmt.Sprintf(`
resource "aws_dynamodb_table" "test" {
  name           = %[1]q
  read_capacity  = 10
  write_capacity = 10
  hash_key       = %[2]q

  attribute {
    name = %[3]q
    type = "S"
  }
}

resource "aws_dynamodb_table_item" "test" {
  table_name = aws_dynamodb_table.test.name
  hash_key   = aws_dynamodb_table.test.hash_key
  item = <<ITEM
%[4]s
ITEM
}

data "aws_dynamodb_table_query" "test" {
	expression_attribute_names = {
    "#P" = "Percentile"
  }
  projection_expression = "#P"
  table_name                  = aws_dynamodb_table.test.name
	key_condition_expression    = "hashKey = :value"
	expression_attribute_values = {
		":value"= "{\"S\": \"something\"}"
	}
  depends_on                  = [aws_dynamodb_table_item.test]
}
`, tableName, hashKey, hashKey, item)
}

func testAccTableQueryDataSourceConfig_select(tableName, item, hashKey, selectValue string) string {
	return fmt.Sprintf(`
resource "aws_dynamodb_table" "test" {
  name           = %q
  read_capacity  = 10
  write_capacity = 10
  hash_key       = %q

  attribute {
    name = %q
    type = "S"
  }
}

resource "aws_dynamodb_table_item" "test" {
  table_name = aws_dynamodb_table.test.name
  hash_key   = aws_dynamodb_table.test.hash_key
  item = <<ITEM
%s
ITEM
}

data "aws_dynamodb_table_query" "test" {
	select = %q
  table_name                  = aws_dynamodb_table.test.name
	key_condition_expression    = "hashKey = :value"
	expression_attribute_values = {
		":value"= "{\"S\": \"something\"}"
	}
  depends_on                  = [aws_dynamodb_table_item.test]
}
`, tableName, hashKey, hashKey, item, selectValue)
}

func testAccTableQueryDataSourceConfig_filterExpression(tableName, item1, item2, filterExpression string) string {
	return fmt.Sprintf(`
resource "aws_dynamodb_table" "test" {
  name           = %q
  read_capacity  = 10
  write_capacity = 10
  hash_key       = "ID"

  attribute {
    name = "ID"
    type = "S"
  }
}

resource "aws_dynamodb_table_item" "item1" {
  table_name = aws_dynamodb_table.test.name
  hash_key   = aws_dynamodb_table.test.hash_key
  item       = %q
}

resource "aws_dynamodb_table_item" "item2" {
  table_name = aws_dynamodb_table.test.name
  hash_key   = aws_dynamodb_table.test.hash_key
  item       = %q
}

data "aws_dynamodb_table_query" "test" {
  table_name                  = aws_dynamodb_table.test.name
	key_condition_expression    = "ID = :value"
  expression_attribute_values = {
    ":value" = "{\"S\": \"something\"}"
		":category" = "{\"S\": \"A\"}"

  }
  filter_expression = %q
  depends_on        = [aws_dynamodb_table_item.item1, aws_dynamodb_table_item.item2]
}
`, tableName, item1, item2, filterExpression)
}

func testAccTableQueryDataSourceConfig_scanIndexForward(tableName, hashKey, sortKey, sortKeyValue1, sortKeyValue2, sortKeyValue3 string, scanIndexForward bool) string {
	return fmt.Sprintf(`
resource "aws_dynamodb_table" "test" {
  name           = %q
  read_capacity  = 10
  write_capacity = 10
  hash_key       = %q
  range_key      = %q

  attribute {
    name = %q
    type = "S"
  }

  attribute {
    name = %q
    type = "S"
  }
}

resource "aws_dynamodb_table_item" "item1" {
  table_name = aws_dynamodb_table.test.name
  hash_key   = aws_dynamodb_table.test.hash_key
  range_key  = aws_dynamodb_table.test.range_key
  item = <<ITEM
{
  "hashKey": {"S": "something"},
  "sortKey": {"S": %q},
  "one": {"N": "11111"}
}
ITEM
}

resource "aws_dynamodb_table_item" "item2" {
  table_name = aws_dynamodb_table.test.name
  hash_key   = aws_dynamodb_table.test.hash_key
  range_key  = aws_dynamodb_table.test.range_key
  item = <<ITEM
{
  "hashKey": {"S": "something"},
  "sortKey": {"S": %q},
  "two": {"N": "22222"}
}
ITEM
}

resource "aws_dynamodb_table_item" "item3" {
  table_name = aws_dynamodb_table.test.name
  hash_key   = aws_dynamodb_table.test.hash_key
  range_key  = aws_dynamodb_table.test.range_key
  item = <<ITEM
{
  "hashKey": {"S": "something"},
  "sortKey": {"S": %q},
  "three": {"N": "33333"}
}
ITEM
}

data "aws_dynamodb_table_query" "test" {
  table_name                = aws_dynamodb_table.test.name
  key_condition_expression  = "hashKey = :hashValue"
  expression_attribute_values = {
    ":hashValue" = "{\"S\": \"something\"}"
  }
  scan_index_forward = %t
  depends_on         = [aws_dynamodb_table_item.item1, aws_dynamodb_table_item.item2, aws_dynamodb_table_item.item3]
}
`, tableName, hashKey, sortKey, hashKey, sortKey, sortKeyValue1, sortKeyValue2, sortKeyValue3, scanIndexForward)
}

func testAccTableQueryDataSourceConfig_index(tableName, item, projectionType, GSIName string) string {
	return fmt.Sprintf(`
	resource "aws_dynamodb_table" "test" {
		name           = %q
		read_capacity  = 10
		write_capacity = 10
		hash_key       = "hashKey"
	
		attribute {
			name = "hashKey"
			type = "S"
		}

		global_secondary_index {
			name            = %q
			hash_key        = "hashKey"
			read_capacity   = 5
			write_capacity  = 5
			projection_type = "INCLUDE"  # Set to "INCLUDE" to include specific attributes in the projection
			non_key_attributes = ["extraAttribute"]  # Include an additional attribute in the GSI

		}
	}
	
	resource "aws_dynamodb_table_item" "item" {
		table_name = aws_dynamodb_table.test.name
		hash_key   = aws_dynamodb_table.test.hash_key
		item = <<ITEM
%s
	ITEM
	}

	data "aws_dynamodb_table_query" "test" {
		table_name               = %q
		key_condition_expression  = "hashKey = :hashValue"
		expression_attribute_values = {
			":hashValue" = "{\"S\": \"something\"}"
		}
		index_name              = %q
		depends_on              = [aws_dynamodb_table_item.item]
	}
`, tableName, GSIName, item, tableName, GSIName)
}

func TestConvertJSONToAttributeValue(t *testing.T) {
	t.Parallel()

	cases := []struct {
		jsonStr     string
		expectedAV  *tfdynamo.AttributeValue
		expectedErr bool
	}{
		{
			jsonStr: "{\"S\":\"example\"}",
			expectedAV: &tfdynamo.AttributeValue{
				S: strPtr("example"),
			},
			expectedErr: false,
		},
		{
			jsonStr: `{"B":"YmFzZTY0IGVuY29kaW5nIGVuY3J5cHRpb24="}`,
			expectedAV: &tfdynamo.AttributeValue{
				B: []byte("base64 encoding encryption"),
			},
			expectedErr: false,
		},
		{
			jsonStr: `{"BOOL":true}`,
			expectedAV: &tfdynamo.AttributeValue{
				BOOL: boolPtr(true),
			},
			expectedErr: false,
		},
		{
			jsonStr: `{"BS":["YmFzZTY0","ZW5jb2Rpbmc="]}`,
			expectedAV: &tfdynamo.AttributeValue{
				BS: [][]byte{[]byte("base64"), []byte("encoding")},
			},
			expectedErr: false,
		},
		{
			jsonStr: `{"L":[{"S":"value1"},{"S":"value2"}]}`,
			expectedAV: &tfdynamo.AttributeValue{
				L: []*tfdynamo.AttributeValue{
					{S: strPtr("value1")},
					{S: strPtr("value2")},
				},
			},
			expectedErr: false,
		},
		{
			jsonStr: `{"M":{"key1":{"S":"value1"},"key2":{"S":"value2"}}}`,
			expectedAV: &tfdynamo.AttributeValue{
				M: map[string]*tfdynamo.AttributeValue{
					"key1": {S: strPtr("value1")},
					"key2": {S: strPtr("value2")},
				},
			},
			expectedErr: false,
		},
		{
			jsonStr: `{"N":"12345"}`,
			expectedAV: &tfdynamo.AttributeValue{
				N: strPtr("12345"),
			},
			expectedErr: false,
		},
		{
			jsonStr: `{"NS":["123","456"]}`,
			expectedAV: &tfdynamo.AttributeValue{
				NS: []*string{strPtr("123"), strPtr("456")},
			},
			expectedErr: false,
		},
		{
			jsonStr: `{"NULL":true}`,
			expectedAV: &tfdynamo.AttributeValue{
				NULL: boolPtr(true),
			},
			expectedErr: false,
		},
		{
			jsonStr: `{"SS":["value1","value2"]}`,
			expectedAV: &tfdynamo.AttributeValue{
				SS: []*string{strPtr("value1"), strPtr("value2")},
			},
			expectedErr: false,
		},
		{
			jsonStr:     `invalidJSON`,
			expectedAV:  nil,
			expectedErr: true,
		},
	}

	for _, c := range cases {
		t.Run(c.jsonStr, func(t *testing.T) {
			result, err := tfdynamo.ConvertJSONToAttributeValue(c.jsonStr)
			log.Println("[ERROR] result: ", result)

			if c.expectedErr && err == nil {
				t.Errorf("Expected error, but got nil")
			}

			if !c.expectedErr && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			if !reflect.DeepEqual(result, c.expectedAV) {
				t.Errorf("Unexpected result. Expected %+v, got %+v", c.expectedAV, result)
			}
		})
	}
}

func TestConvertToDynamoAttributeValue(t *testing.T) {
	t.Parallel()

	cases := []struct {
		av          *tfdynamo.AttributeValue
		expectedAV  *dynamodb.AttributeValue
		expectedErr bool
	}{
		{
			av: &tfdynamo.AttributeValue{
				S: strPtr("example"),
			},
			expectedAV: &dynamodb.AttributeValue{
				S: strPtr("example"),
			},
			expectedErr: false,
		},
		{
			av: &tfdynamo.AttributeValue{
				B: []byte("base64 encoding encryption"),
			},
			expectedAV: &dynamodb.AttributeValue{
				B: []byte("base64 encoding encryption"),
			},
			expectedErr: false,
		},
		{
			av: &tfdynamo.AttributeValue{
				BOOL: boolPtr(true),
			},
			expectedAV: &dynamodb.AttributeValue{
				BOOL: boolPtr(true),
			},
			expectedErr: false,
		},
		{
			av: &tfdynamo.AttributeValue{
				BS: [][]byte{[]byte("base64"), []byte("encoding")},
			},
			expectedAV: &dynamodb.AttributeValue{
				BS: [][]byte{[]byte("base64"), []byte("encoding")},
			},
			expectedErr: false,
		},
		{
			av: &tfdynamo.AttributeValue{
				L: []*tfdynamo.AttributeValue{
					{S: strPtr("value1")},
					{S: strPtr("value2")},
				},
			},
			expectedAV: &dynamodb.AttributeValue{
				L: []*dynamodb.AttributeValue{
					{S: strPtr("value1")},
					{S: strPtr("value2")},
				},
			},
			expectedErr: false,
		},
		{
			av: &tfdynamo.AttributeValue{
				M: map[string]*tfdynamo.AttributeValue{
					"key1": {S: strPtr("value1")},
					"key2": {S: strPtr("value2")},
				},
			},
			expectedAV: &dynamodb.AttributeValue{
				M: map[string]*dynamodb.AttributeValue{
					"key1": {S: strPtr("value1")},
					"key2": {S: strPtr("value2")},
				},
			},
			expectedErr: false,
		},
		{
			av: &tfdynamo.AttributeValue{
				N: strPtr("12345"),
			},
			expectedAV: &dynamodb.AttributeValue{
				N: strPtr("12345"),
			},
			expectedErr: false,
		},
		{
			av: &tfdynamo.AttributeValue{
				NS: []*string{strPtr("123"), strPtr("456")},
			},
			expectedAV: &dynamodb.AttributeValue{
				NS: []*string{strPtr("123"), strPtr("456")},
			},
			expectedErr: false,
		},
		{
			av: &tfdynamo.AttributeValue{
				NULL: boolPtr(true),
			},
			expectedAV: &dynamodb.AttributeValue{
				NULL: boolPtr(true),
			},
			expectedErr: false,
		},
		{
			av: &tfdynamo.AttributeValue{
				SS: []*string{strPtr("value1"), strPtr("value2")},
			},
			expectedAV: &dynamodb.AttributeValue{
				SS: []*string{strPtr("value1"), strPtr("value2")},
			},
			expectedErr: false,
		},
		{
			av:          nil,
			expectedAV:  nil,
			expectedErr: false,
		},
	}

	for _, c := range cases {
		t.Run("", func(t *testing.T) {
			result, err := tfdynamo.ConvertToDynamoAttributeValue(c.av)

			if c.expectedErr && err == nil {
				t.Errorf("Expected error, but got nil")
			}

			if !c.expectedErr && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			if !reflect.DeepEqual(result, c.expectedAV) {
				t.Errorf("Unexpected result. Expected %+v, got %+v", c.expectedAV, result)
			}
		})
	}
}

func strPtr(s string) *string {
	return &s
}

func boolPtr(b bool) *bool {
	return &b
}
