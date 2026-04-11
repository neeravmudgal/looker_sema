view: session_purchase_facts {
  derived_table: {
    datagroup_trigger: ecommerce_etl
    sql:
      with session_purchase as (
      select
      coalesce(session_rank - lag(session_rank) over(partition by session_user_id order by session_end), session_rank) as sessions_till_purchase
      , session_rank as rank
      , rank() over (partition by session_user_id order by session_end) as session_purchase_rank
      , lag(session_end) over(partition by session_user_id order by session_end) as purchase_session_start
      ,*
      from ${sessions.SQL_TABLE_NAME}
      where purchase_events > 0
      order by session_user_id, session_rank
    )
    ,
     session_contains_search as (
     select
       session_purchase.session_id,
       sum(case when sessions.traffic_source = 'Adwords' then 1 else 0 end) as search_sessions
     from session_purchase
     join ${sessions.SQL_TABLE_NAME}  as sessions
     on session_purchase.session_user_id = sessions.session_user_id and sessions.session_start >= session_purchase.purchase_session_start and sessions.session_end <= session_purchase.session_end
     group by 1

     )
    select

        *,
          COALESCE(lag(session_end) over (partition by session_user_id order by session_user_id, session_start), '0001-01-01 00:00:00') as last_session_end
        , rank() over (partition by session_user_id order by session_end) as session_purchase_rank
    from (
      SELECT
        events.session_id
        , order_id
        , session_purchase.traffic_source as purchase_session_traffic_source
        , sum(sessions_till_purchase) as sessions_till_purchase
        , sum(sale_price) AS sale_price
        --, sum(inventory_items.cost) as cost
        , sum(search_sessions) as search_session_count
        , MIN(events.created_at) AS session_start
        , MAX(events.created_at) AS session_end
        , MAX(events.user_id) AS session_user_id
      FROM ecomm.events
      JOIN ecomm.order_items on order_items.created_at = events.created_at
      --JOIN ecomm.inventory_items  AS inventory_items ON inventory_items.id = order_items.inventory_item_id
      JOIN session_purchase on session_purchase.session_id = events.session_id
      JOIN session_contains_search on session_purchase.session_id = session_contains_search.session_id
      GROUP BY events.session_id, order_id, session_purchase.traffic_source
      having sum(CASE WHEN event_type = 'Purchase' THEN 1 else 0 end) > 0
      order by session_user_id
    )
    ;;
  }

  dimension: session_id {
    description: "Unique session identifier for sessions that resulted in a purchase"
    hidden: yes
    primary_key: yes
    type: string
    sql: ${TABLE}.session_id ;;
  }

  dimension: order_id {
    description: "Unique order identifier for the purchase made during this session"
    hidden: yes
    type: number
    sql: ${TABLE}.order_id ;;
  }

  dimension: purchases_per_session {
    description: "Fractional purchase attribution per session, calculated as 1 divided by the number of sessions leading to this purchase"
    view_label: "Sessions"
    hidden: yes
    type: number
    sql: 1.0 * 1.0 /nullif(${sessions_till_purchase},0 );;
    value_format_name: decimal_0
    drill_fields: [attribution_detail*]
  }

  measure: total_purchases {
    description: "Total number of purchases attributed to these sessions using linear attribution"
    view_label: "Sessions"
    label: "Purchases"
    type: sum_distinct
    sql_distinct_key: ${sessions.session_id} ;;
    sql: ${purchases_per_session} ;;
    value_format_name: decimal_0
    drill_fields: [attribution_detail*]
  }

  dimension: purchase_session_source {
    view_label: "Sessions"
    description: "Last Touch Attribution: Source of last session before purchase"
    type: string
    sql: ${TABLE}.purchase_session_traffic_source ;;
  }

  dimension: sessions_till_purchase {
    description: "Number of sessions the user had between their previous purchase and this purchase"
    hidden: yes
    type: number
    sql: ${TABLE}.sessions_till_purchase ;;
  }

  dimension: sale_price {
    description: "Total sale price of items purchased in this session"
    hidden: yes
    type: number
    sql: ${TABLE}.sale_price ;;
  }

  dimension: purchase_pk {
    description: "Composite primary key combining purchase rank and user ID for unique purchase identification"
    hidden: yes
    sql: cast(${session_purchase_rank} as varchar) + cast(${session_user_id} as varchar) ;;
  }

  measure: revenue {
    description: "Total revenue from all purchases across sessions"
    view_label: "Sessions"
    label:  "Revenue"
    type: sum
    value_format_name: usd
    sql: ${sale_price} ;;
    drill_fields: [attribution_detail*]
  }

  measure: ROI {
    description: "Return on investment calculated as (Revenue / Ad Spend) - 1, expressed as a percentage"
    view_label: "Sessions"
    label: "ROI (Revenue/Cost)"
    type: number
    value_format_name: percent_1
    sql: trunc(1.0 * ${revenue}/ NULLIF(${adevents.total_cost},0) - 1,3) ;;
    drill_fields: [attribution_detail*]
  }

  measure: net_profit {
    description: "Net profit calculated as total revenue minus total ad spend"
    view_label: "Sessions"
    label: "Profit"
    type: number
    value_format_name: usd
    sql: ${revenue} - ${adevents.total_cost} ;;
    drill_fields: [attribution_detail*]
  }

  dimension: session_purchase_rank {
    description: "Sequential rank of this purchase within the user's purchase history (1st purchase, 2nd purchase, etc.)"
    hidden: yes
    view_label: "Sessions"
    type: number
    sql: ${TABLE}.session_purchase_rank ;;
  }

  dimension_group: last_session_end {
    description: "End time of the previous purchase session, marking the start of the attribution window for this purchase"
    label: "Purchase Start Session"
    view_label: "Sessions"
    type: time
    timeframes: [raw, time, date, week, month]
    sql: ${TABLE}.last_session_end;;
  }

  dimension_group: session_end {
    description: "End time of the purchase session, marking the end of the attribution window"
    type: time
    view_label: "Sessions"
    label: "Purchase End Session"
    timeframes: [raw, time, date, week, month]
    sql: ${TABLE}.session_end ;;
  }

  dimension_group: session_start {
    description: "Start time of the purchase session"
    hidden: yes
    type: time
    timeframes: [raw, time, date, week, month]
    sql: ${TABLE}.session_start ;;
  }

  dimension: session_user_id {
    description: "Foreign key to the user who made the purchase"
    hidden: yes
    type: number
    sql: ${TABLE}.session_user_id ;;
  }

#   ----------------

  set: attribution_detail {
    fields: [
      campaigns.campaign_name,
      adevents.total_cost,
      sessions.purchases,
      revenue,
      events.bounce_rate
    ]
  }

  set: detail {
    fields: [
      session_id,
      session_start_time,
      session_end_time,
      session_user_id,
      last_session_end_time,
      revenue
    ]
  }
}
