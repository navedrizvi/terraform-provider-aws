// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package dynamodb_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	sdkacctest "github.com/hashicorp/terraform-plugin-testing/helper/acctest"
	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-provider-aws/internal/acctest"
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
	// scanIndexForward := false
	scanIndexForward := true

	expected := `{
		"one": {"N": "11111"},
		"two": {"N": "22222"},
		"three": {"N": "33333"}
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
				Config: testAccTableQueryDataSourceConfig_scanIndexForward(rName, hashKey, sortKey, sortKeyValue1, sortKeyValue2, sortKeyValue3, scanIndexForward),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(dataSourceName, "items.#", "3"),
					acctest.CheckResourceAttrEquivalentJSON(dataSourceName, "items.0", expected),
					resource.TestCheckResourceAttr(dataSourceName, "table_name", rName),
					resource.TestCheckResourceAttr(dataSourceName, "scan_index_forward", "false"),
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
		":value"= "{\"S\": \"something\"}"
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
	select = %[5]q
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
  name           = %[1]q
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
  item       = %[2]q
}

resource "aws_dynamodb_table_item" "item2" {
  table_name = aws_dynamodb_table.test.name
  hash_key   = aws_dynamodb_table.test.hash_key
  item       = %[3]q
}

data "aws_dynamodb_table_query" "test" {
  table_name                  = aws_dynamodb_table.test.name
	key_condition_expression    = "ID = :value"
  expression_attribute_values = {
    ":value" = "{\"S\": \"something\"}"
		":category" = "{\"S\": \"A\"}"

  }
  filter_expression = %[4]q
  depends_on        = [aws_dynamodb_table_item.item1, aws_dynamodb_table_item.item2]
}
`, tableName, item1, item2, filterExpression)
}

func testAccTableQueryDataSourceConfig_scanIndexForward(tableName, hashKey, sortKey, sortKeyValue1, sortKeyValue2, sortKeyValue3 string, scanIndexForward bool) string {
	a := fmt.Sprintf(`
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

	fmt.Printf("[ERROR]2 AAA: %v\n", a)
	return a
}

// func testAccTableQueryDataSourceConfig_scanIndexForward(tableName, hashKey, sortKey, sortKeyValue1, sortKeyValue2, sortKeyValue3 string, scanIndexForward bool) string {
// 	a := fmt.Sprintf(`
// resource "aws_dynamodb_table" "test" {
//   name           = %[1]q
//   read_capacity  = 10
//   write_capacity = 10
//   hash_key       = %[2]q
//   range_key       = %[3]q

//   attribute {
//     name = %[2]q
//     type = "S"
//   }

//   attribute {
//     name = %[3]q
//     type = "S"
//   }
// }

// resource "aws_dynamodb_table_item" "item1" {
//   table_name = aws_dynamodb_table.test.name
//   hash_key   = aws_dynamodb_table.test.hash_key
//   range_key   = aws_dynamodb_table.test.range_key
//   item = <<ITEM
// {
//   "hashKey": {"S": "something"},
//   "sortKey": {"S": "%[4]s"},
//   "one": {"N": "11111"}
// }
// ITEM
// }

// resource "aws_dynamodb_table_item" "item2" {
//   table_name = aws_dynamodb_table.test.name
//   hash_key   = aws_dynamodb_table.test.hash_key
//   range_key   = aws_dynamodb_table.test.range_key
//   item = <<ITEM
// {
//   "hashKey": {"S": "something"},
//   "sortKey": {"S": "%[5]s"},
//   "two": {"N": "22222"}
// }
// ITEM
// }

// resource "aws_dynamodb_table_item" "item3" {
//   table_name = aws_dynamodb_table.test.name
//   hash_key   = aws_dynamodb_table.test.hash_key
//   range_key   = aws_dynamodb_table.test.range_key
//   item = <<ITEM
// {
//   "hashKey": {"S": "something"},
//   "sortKey": {"S": "%[6]s"},
//   "three": {"N": "33333"}
// }
// ITEM
// }

// data "aws_dynamodb_table_query" "test" {
//   table_name                = aws_dynamodb_table.test.name
//   key_condition_expression  = "hashKey = :hashValue"
//   expression_attribute_values = {
//     ":hashValue" = "{\"S\": \"something\"}"
//   }
//   scan_index_forward = %[7]t
//   depends_on         = [aws_dynamodb_table_item.item1, aws_dynamodb_table_item.item2, aws_dynamodb_table_item.item3]
// }
// `, tableName, hashKey, sortKey, sortKeyValue1, sortKeyValue2, sortKeyValue3, scanIndexForward)

// 	fmt.Printf("[ERROR]2 AAA: %v\n", a)
// 	return a
// }
