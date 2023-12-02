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
	hashKeyValue := "something"
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
				Config: testAccTableQueryDataSourceConfig_basic(rName, hashKey, itemContent, hashKeyValue),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(dataSourceName, "items.#", "1"),
					acctest.CheckResourceAttrEquivalentJSON(dataSourceName, "items.0", itemContent),
					resource.TestCheckResourceAttr(dataSourceName, "table_name", rName),
					resource.TestCheckResourceAttr(dataSourceName, "scanned_count", "1"),
					resource.TestCheckResourceAttr(dataSourceName, "item_count", "1"),
				),
			},
		},
	})
}

// // TODO1 - refactor this to also accept other req params?
func TestAccDynamoDBTableQueryDataSource_consumedCapacity(t *testing.T) {
	ctx := acctest.Context(t)
	rName := sdkacctest.RandomWithPrefix(acctest.ResourcePrefix)
	dataSourceName := "data.aws_dynamodb_table_query.test"
	hashKey := "hashKey"
	hashKeyValue := "something"
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
				Config: testAccTableQueryDataSourceConfig_returnConsumedCapacity(rName, hashKey, itemContent, hashKeyValue),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttrSet(dataSourceName, "consumed_capacity"),

					resource.TestCheckResourceAttr(dataSourceName, "items.#", "1"),
					acctest.CheckResourceAttrEquivalentJSON(dataSourceName, "items.0", itemContent),
					resource.TestCheckResourceAttr(dataSourceName, "table_name", rName),
					resource.TestCheckResourceAttr(dataSourceName, "scanned_count", "1"),
					resource.TestCheckResourceAttr(dataSourceName, "item_count", "1"),
				),
			},
		},
	})
}

// TODO1 - pass key_condition_expression as param?
func testAccTableQueryDataSourceConfig_basic(tableName, hashKey, item, hashKeyValue string) string {
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
`, tableName, hashKey, hashKey, item, hashKeyValue)
}

// expression_attribute_values = {":value" = "something"}
// expression_attribute_values = {":value"= "{\"S\": \"something\"}"}

func testAccTableQueryDataSourceConfig_returnConsumedCapacity(tableName, hashKey, item, hashKeyValue string) string {
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
	return_consumed_capacity		= "TOTAL"
  table_name                  = aws_dynamodb_table.test.name
	key_condition_expression    = "hashKey = :value"
	expression_attribute_values = {":value"= "{\"S\": \"something\"}"}
  depends_on                  = [aws_dynamodb_table_item.test]
}
`, tableName, hashKey, hashKey, item, hashKeyValue)
}
