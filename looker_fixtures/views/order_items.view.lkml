view: order_items {
  sql_table_name: ecomm.order_items ;;

  dimension: id {
    description: "Unique identifier for each order line item"
    primary_key: yes
    type: number
    sql: ${TABLE}.ID ;;
  }

  dimension_group: created {
    description: "Timestamp when the order item was placed"
    #X# group_label:"Order Date"
    type: time
    timeframes: [time, hour, date, week, month, year, hour_of_day, day_of_week, month_num, raw, week_of_year]
    sql: ${TABLE}.CREATED_AT ;;
  }

  dimension: delivered_at {
    description: "Timestamp when the order item was delivered to the customer"
    type: string
    sql: ${TABLE}.DELIVERED_AT ;;
  }

  dimension: inventory_item_id {
    description: "Foreign key to the specific inventory item fulfilled for this order line"
    type: number
    sql: ${TABLE}.INVENTORY_ITEM_ID ;;
  }

  dimension: order_id {
    description: "Identifier for the parent order this line item belongs to — multiple items can share the same order ID"
    type: number
    sql: ${TABLE}.ORDER_ID ;;
  }

  dimension: returned_at {
    description: "Timestamp when the order item was returned, null if not returned"
    type: string
    sql: ${TABLE}.RETURNED_AT ;;
  }

  dimension: sale_price {
    description: "Actual selling price of the item in USD after any discounts"
    type: number
    sql: ${TABLE}.SALE_PRICE ;;
  }

  dimension: shipped_at {
    description: "Timestamp when the order item was shipped from the warehouse"
    type: string
    sql: ${TABLE}.SHIPPED_AT ;;
  }

  dimension: status {
    description: "Current fulfillment status of the order item (e.g., Complete, Returned, Processing)"
    type: string
    sql: ${TABLE}.STATUS ;;
  }

  dimension: user_id {
    description: "Foreign key to the user who placed the order"
    type: number
    sql: ${TABLE}.USER_ID ;;
  }

  measure: total_sale_price {
    description: "Sum of all order item sale prices in USD"
    type: sum
    sql: ${sale_price} ;;
    value_format_name: usd_0
  }

  measure: count {
    description: "Total number of order line items"
    type: count
    drill_fields: [id]
  }
}
