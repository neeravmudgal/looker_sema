view: sessions {
  derived_table: {
    datagroup_trigger: ecommerce_etl
    sql:
      SELECT
        row_number() over (partition by session_user_id order by session_end) as session_rank
        ,CASE WHEN purchase_events > 0
              THEN row_number() over (partition by session_user_id order by session_end)
              ELSE null
          END AS purchase_rank
        , *
      FROM(
      SELECT
        session_id
        , MIN(created_at) AS session_start
        , MAX(created_at) AS session_end
        , COUNT(*) AS number_of_events_in_session
        , SUM(CASE WHEN event_type IN ('Category','Brand') THEN 1 else 0 END) AS browse_events
        , SUM(CASE WHEN event_type = 'Product' THEN 1 else 0 END) AS product_events
        , SUM(CASE WHEN event_type = 'Cart' THEN 1 else 0 END) AS cart_events
        , SUM(CASE WHEN event_type = 'Purchase' THEN 1 else 0 end) AS purchase_events
        , MAX(user_id) AS session_user_id
        , MIN(id) AS landing_event_id
        , MAX(id) AS bounce_event_id
        , MAX(traffic_source) AS traffic_source
        , MAX(ad_event_id) AS ad_event_id
      FROM ecomm.events
      GROUP BY session_id
      )
;;
  }

  #####  Basic Web Info  ########

  dimension: session_id {
    description: "Unique identifier for each website session"
    type: string
    hidden: yes
    primary_key: yes
    sql: ${TABLE}.session_id ;;
  }

  dimension: traffic_source {
    description: "The marketing channel or source that drove this session (e.g., Organic, Adwords, Email, Facebook, YouTube)"
    type: string
  }

  dimension: ad_event_id {
    description: "Foreign key to the ad event that initiated this session, if the session originated from a paid ad"
    type: number
  }

  dimension: session_rank {
    description: "Sequential rank of this session within the user's visit history (1 = first ever session)"
    type: number
    sql: ${TABLE}.session_rank ;;
  }

  dimension: purchase_rank {
    description: "Sequential rank of this session among sessions with purchases (null if no purchase occurred)"
    type: number
    sql: ${TABLE}.purchase_rank ;;
  }

  dimension: session_type {
    description: "Static value 'All' used for pivot-based analyses to compare session segments"
    type: string
    sql: 'All' ;;
  }

  dimension: session_user_id {
    description: "Foreign key to the user who initiated this session"
    type: number
    sql: ${TABLE}.session_user_id ;;
  }

  dimension: landing_event_id {
    description: "Event ID of the first event in the session, identifying the landing page"
    sql: ${TABLE}.landing_event_id ;;
  }

  dimension: bounce_event_id {
    description: "Event ID of the last event in the session — equals landing_event_id for single-event bounce sessions"
    sql: ${TABLE}.bounce_event_id ;;
  }

  dimension_group: session_start {
    description: "Timestamp when the session began (first event in the session)"
    type: time
    timeframes: [raw, time, date, week, month, quarter, hour_of_day, day_of_week]
    sql: ${TABLE}.session_start ;;
  }

  dimension_group: session_end {
    description: "Timestamp when the session ended (last event in the session)"
    type: time
    timeframes: [raw, time, date, week, month,quarter]
    sql: ${TABLE}.session_end ;;
  }

  dimension: duration {
    description: "Session duration in seconds, calculated as the time between first and last events"
    label: "Duration (sec)"
    type: number
    sql: DATEDIFF('second', ${session_start_raw}, ${session_end_raw}) ;;
  }

  measure: average_duration {
    description: "Average session duration in seconds across all sessions"
    label: "Average Duration (sec)"
    type: average
    value_format_name: decimal_2
    sql: ${duration} ;;
  }

  dimension: duration_seconds_tier {
    description: "Session duration bucketed into tiers: 0-9s, 10-29s, 30-59s, 60-119s, 120-299s, 300+s"
    label: "Duration Tier (sec)"
    type: tier
    tiers: [10, 30, 60, 120, 300]
    style: integer
    sql: ${duration} ;;
  }

  dimension: months_since_first_session {
    description: "Number of months between the user's account creation date and this session, indicating user maturity"
    type: number
    sql: datediff( 'month', ${users.created_raw}, ${session_start_raw} ) ;;
  }

  measure: count {
    description: "Total number of sessions"
    type: count
    drill_fields: [detail*]
  }

  measure: spend_per_session {
    description: "Average advertising cost per session, calculated as total ad spend divided by session count"
    hidden: yes
    type: number
    value_format_name: usd
    sql: 1.0*${adevents.total_cost} / NULLIF(${count},0) ;;
    drill_fields: [detail*]
  }

  measure: spend_per_purchase {
    description: "Average advertising cost per purchase session, calculated as total ad spend divided by sessions with purchases"
    hidden: yes
    type: number
    value_format_name: usd
    sql: 1.0*${adevents.total_cost} / NULLIF(${count_with_purchase},0) ;;
    drill_fields: [detail*]
  }

  #####  Bounce Information  ########

  dimension: is_bounce_session {
    description: "Yes if the session had only one event (user landed and left without further interaction)"
    type: yesno
    sql: ${number_of_events_in_session} = 1 ;;
  }

  measure: count_bounce_sessions {
    description: "Total number of sessions where the user left after viewing only one page"
    type: count

    filters: {
      field: is_bounce_session
      value: "Yes"
    }
    drill_fields: [detail*]
  }

  measure: percent_bounce_sessions {
    description: "Percentage of all sessions that were bounces (single-page visits)"
    type: number
    value_format_name: percent_2
    sql: 1.0 * ${count_bounce_sessions} / nullif(${count},0) ;;
  }

  ####### Session by event types included  ########

  dimension: number_of_browse_events_in_session {
    description: "Count of Category or Brand browsing events within this session"
    type: number
    hidden: yes
    sql: ${TABLE}.browse_events ;;
  }

  dimension: number_of_product_events_in_session {
    description: "Count of product detail page views within this session"
    type: number
    hidden: yes
    sql: ${TABLE}.product_events ;;
  }

  dimension: number_of_cart_events_in_session {
    description: "Count of add-to-cart events within this session"
    type: number
    hidden: yes
    sql: ${TABLE}.cart_events ;;
  }

  dimension: number_of_purchase_events_in_session {
    description: "Count of purchase/checkout events within this session"
    type: number
    hidden: yes
    sql: ${TABLE}.purchase_events ;;
  }

  dimension: includes_browse {
    description: "Yes if the session included at least one category or brand browsing event"
    type: yesno
    sql: ${number_of_browse_events_in_session} > 0 ;;
  }

  dimension: includes_product {
    description: "Yes if the session included at least one product detail page view"
    type: yesno
    sql: ${number_of_product_events_in_session} > 0 ;;
  }

  dimension: includes_cart {
    description: "Yes if the session included at least one add-to-cart event"
    type: yesno
    sql: ${number_of_cart_events_in_session} > 0 ;;
  }

  dimension: includes_purchase {
    description: "Yes if the session included at least one purchase/checkout event"
    type: yesno
    sql: ${number_of_purchase_events_in_session} > 0 ;;
  }

  dimension: weeks_since_campaing_start {
    label: "Weeks Since Campaign Start"
    description:  "Weeks between campaign start and user's session start (e.g. first click)"
    view_label: "Campaigns"
    type: number
    sql: DATEDIFF('week', ${campaigns.created_date}, ${session_start_date})  ;;
  }

  measure: count_with_cart {
    description: "Total number of sessions that included at least one add-to-cart event"
    type: count

    filters: {
      field: includes_cart
      value: "Yes"
    }

    drill_fields: [detail*]
  }

  measure: count_with_purchase {
    description: "Total number of sessions that resulted in at least one purchase"
    type: count

    filters: {
      field: includes_purchase
      value: "Yes"
    }

    drill_fields: [detail*]
  }

  dimension: number_of_events_in_session {
    description: "Total count of all events (page views and actions) that occurred during this session"
    type: number
    sql: ${TABLE}.number_of_events_in_session ;;
  }

  ####### Linear Funnel   ########

  dimension: furthest_funnel_step {
    description: "The deepest stage the user reached in the purchase funnel during this session: (1) Land, (2) Browse, (3) View Product, (4) Add to Cart, or (5) Purchase"
    sql: CASE
      WHEN ${number_of_purchase_events_in_session} > 0 THEN '(5) Purchase'
      WHEN ${number_of_cart_events_in_session} > 0 THEN '(4) Add to Cart'
      WHEN ${number_of_product_events_in_session} > 0 THEN '(3) View Product'
      WHEN ${number_of_browse_events_in_session} > 0 THEN '(2) Browse'
      ELSE '(1) Land'
      END
       ;;
  }

  measure: all_sessions {
    description: "Total count of all sessions, representing the top of the conversion funnel"
    view_label: "Funnel View"
    label: "(1) All Sessions"
    type: count
    drill_fields: [detail*]
  }

  measure: count_browse_or_later {
    description: "Sessions where the user browsed categories/brands or progressed further in the funnel"
    view_label: "Funnel View"
    label: "(2) Browse or later"
    type: count

    filters: {
      field: furthest_funnel_step
      value: "(2) Browse,(3) View Product,(4) Add to Cart,(5) Purchase
      "
    }
    drill_fields: [detail*]
  }

  measure: count_product_or_later {
    description: "Sessions where the user viewed a product detail page or progressed further in the funnel"
    view_label: "Funnel View"
    label: "(3) View Product or later"
    type: count

    filters: {
      field: furthest_funnel_step
      value: "(3) View Product,(4) Add to Cart,(5) Purchase
      "
    }

    drill_fields: [detail*]
  }

  measure: count_cart_or_later {
    description: "Sessions where the user added an item to cart or completed a purchase"
    view_label: "Funnel View"
    label: "(4) Add to Cart or later"
    type: count

    filters: {
      field: furthest_funnel_step
      value: "(4) Add to Cart,(5) Purchase
      "
    }

    drill_fields: [detail*]
  }

  measure: count_purchase {
    description: "Sessions where the user completed a purchase — the bottom of the conversion funnel"
    view_label: "Funnel View"
    label: "(5) Purchase"
    type: count

    filters: {
      field: furthest_funnel_step
      value: "(5) Purchase
      "
    }

    drill_fields: [detail*]
  }

  measure: cart_to_checkout_conversion {
    description: "Conversion rate from cart to purchase, calculated as purchase sessions divided by cart-or-later sessions"
    view_label: "Funnel View"
    type: number
    value_format_name: percent_2
    sql: 1.0 * ${count_purchase} / nullif(${count_cart_or_later},0) ;;
  }

  measure: overall_conversion {
    description: "Overall session-to-purchase conversion rate, calculated as purchase sessions divided by all sessions"
    view_label: "Funnel View"
    type: number
    value_format_name: percent_2
    sql: 1.0 * ${count_purchase} / nullif(${count},0) ;;
  }

  ### Acquisition Info
  dimension: is_first {
    description: "Yes if this is the user's very first session on the site"
    hidden: yes
    type: yesno
    sql: ${session_rank} = 1 ;;
  }
  dimension: is_first_purchase {
    description: "Yes if this is the user's first session that included a purchase"
    hidden: yes
    type: yesno
    sql: ${purchase_rank} = 1 ;;
  }
  measure: site_acquisition_source {
    description: "The traffic source of the user's very first session, representing how they were originally acquired"
    hidden: yes
    type: string
    sql: max(case when ${is_first} then ${sessions.traffic_source} else null end) ;;
  }
  measure: site_acquisition_ad_event_id {
    description: "The ad event ID from the user's first session, linking to the ad that originally acquired them"
    hidden: yes
    type: number
    sql: max(case when ${is_first} then ${sessions.ad_event_id} else null end) ;;
  }
  measure: first_visit_dt {
    description: "Timestamp of the user's very first session start"
    hidden: yes
    type: number
    sql: min(case when ${is_first} then ${sessions.session_start_raw} else null end) ;;
  }
  measure: first_purchase_dt {
    description: "Timestamp of the user's first purchase session start"
    type: string
    hidden: yes
    sql: min(case when ${is_first_purchase} then ${sessions.session_start_raw} else null end) ;;
  }


  set: funnel_view {
    fields: [
      all_sessions,
      count_browse_or_later,
      count_product_or_later,
      count_cart_or_later,
      count_purchase,
      cart_to_checkout_conversion,
      overall_conversion
    ]
  }

  set: detail {
    fields: [session_id,
      session_start_time,
      session_end_time,
      number_of_events_in_session,
      duration,
      number_of_purchase_events_in_session,
      number_of_cart_events_in_session]
  }
}
