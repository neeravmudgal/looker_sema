view: inventory_items {
  sql_table_name: ecomm.inventory_items ;;

  dimension: id {
    description: "Unique identifier for each inventory item"
    primary_key: yes
    type: number
    sql: ${TABLE}.id ;;
  }

  dimension: cost {
    description: "Wholesale cost of the inventory item in USD"
    type: number
    sql: ${TABLE}.cost ;;
  }

  dimension_group: created {
    description: "Timestamp when the inventory item was added to stock"
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.created_at ;;
  }

  dimension: product_brand {
    description: "Brand name of the product (e.g., Nike, Levi's)"
    type: string
    sql: ${TABLE}.product_brand ;;
  }

  dimension: product_category {
    description: "Product category classification (e.g., Jeans, Accessories, Tops)"
    type: string
    sql: ${TABLE}.product_category ;;
  }

  dimension: product_department {
    description: "Department the product belongs to (e.g., Men, Women)"
    type: string
    sql: ${TABLE}.product_department ;;
  }

  dimension: product_distribution_center_id {
    description: "Foreign key to the distribution center where this inventory item is stored"
    type: number
    sql: ${TABLE}.product_distribution_center_id ;;
  }

  dimension: product_id {
    description: "Foreign key to the product catalog entry for this inventory item"
    type: number
    sql: ${TABLE}.product_id ;;
  }

  dimension: product_name {
    description: "Full display name of the product"
    type: string
    sql: ${TABLE}.product_name ;;
  }

  dimension: product_retail_price {
    description: "Retail selling price of the product in USD"
    type: number
    sql: ${TABLE}.product_retail_price ;;
  }

  dimension: product_sku {
    description: "Stock Keeping Unit — unique product identifier used for inventory tracking"
    type: string
    sql: ${TABLE}.product_sku ;;
  }

  dimension_group: sold {
    description: "Timestamp when the inventory item was sold to a customer"
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.sold_at ;;
  }

  measure: count {
    description: "Total number of inventory items"
    type: count
    drill_fields: [id, product_name]
  }
}
