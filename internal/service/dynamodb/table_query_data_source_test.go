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
	"two": {"N": "22222"},
	"three": {"N": "33333"},
	"four": {"N": "44444"}
}`
	key := `{
	"hashKey": {"S": "something"}
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
				Config: testAccTableQueryDataSourceConfig_basic(rName, hashKey, itemContent, key),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(dataSourceName, "items.#", "1"),
					acctest.CheckResourceAttrEquivalentJSON(dataSourceName, "items.0", itemContent),
					resource.TestCheckResourceAttr(dataSourceName, "table_name", rName),
					// TODO0
					// resource.TestCheckResourceAttrSet(dataSourceName, "last_evaluated_key"),
					resource.TestCheckResourceAttr(dataSourceName, "scanned_count", "1"),
					// resource.TestCheckResourceAttrSet(dataSourceName, "consumed_capacity"),
					resource.TestCheckResourceAttr(dataSourceName, "item_count", "1"),
				),
			},
		},
	})
}

// TODO0

func testAccTableQueryDataSourceConfig_basic(tableName, hashKey, item string, key string) string {
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

data "aws_dynamodb_table_item" "test" {
  table_name = aws_dynamodb_table.test.name
  key        = <<KEY
%[5]s
KEY
  depends_on = [aws_dynamodb_table_item.test]
}

data "aws_dynamodb_table_query" "test" {
  table_name                  = aws_dynamodb_table.test.name
	key_condition_expression    = "hashKey = :hashKey"
	expression_attribute_values = {":hashKey" : "something"}
  depends_on                  = [aws_dynamodb_table_item.test]
}
`, tableName, hashKey, hashKey, item, key, hashKey)
}

// func testAccTableQueryDataSourceConfig_(tableName, hashKey, item string, key string) string {
// 	return fmt.Sprintf(`
// resource "aws_dynamodb_table" "test" {
//   name           = %[1]q
//   read_capacity  = 10
//   write_capacity = 10
//   hash_key       = %[2]q

//   attribute {
//     name = %[3]q
//     type = "S"
//   }
// }

// resource "aws_dynamodb_table_item" "test" {
//   table_name = aws_dynamodb_table.test.name
//   hash_key   = aws_dynamodb_table.test.hash_key
//   item = <<ITEM
// %[4]s
// ITEM
// }

// data "aws_dynamodb_table_item" "test" {
//   table_name = aws_dynamodb_table.test.name
//   key        = <<KEY
// %[5]s
// KEY
//   depends_on = [aws_dynamodb_table_item.test]
// }

// data "aws_dynamodb_table_query" "test" {
//   table_name                  = aws_dynamodb_table.test.name
// 	key_condition_expression    = "hashKey = :hashKey"
// 	expression_attribute_values = {":hashKey" : "something"}
// }
// `, tableName, hashKey, hashKey, item, key, hashKey)
// }

// expression_attribute_values = {
// 		":hashKey" = jsonencode({
// 			"S" = "Available"
// 		})
// 	}
//  ":value" = {"S" = "Available"}
// ":value" = "{\"S\":\"Available\"}"
// expression_attribute_names = ""
// expression_attribute_values = { ":value" : "something"}
// data "aws_dynamodb_table_query" "test" {
//   table_name                  = aws_dynamodb_table.test.name
//   key_condition_expression    = "%[6]s = :value"
//   expression_attribute_values = { ":value" : "something"}
// }
// data "aws_dynamodb_table_query" "test" {
//   table_name = aws_dynamodb_table.test.name

//   key_condition_expression = "%[6]s = :hash_key"
//   expression_attribute_values = {
//     ":hash_key" = %[7]s
//   }
// }

// resource "aws_dynamodb_table_item" "test" {
// 	table_name = aws_dynamodb_table.test.name
// 	hash_key   = aws_dynamodb_table.test.hash_key

// 	item = <<ITEM
// %[4]s
// ITEM
// }

// data "aws_dynamodb_table_item" "test" {
// 	table_name = aws_dynamodb_table.test.name

// 	key        = <<KEY
// %[5]s
// KEY
// 	depends_on = [aws_dynamodb_table_item.test]
// }
