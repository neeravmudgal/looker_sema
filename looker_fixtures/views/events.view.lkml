view: events {
  sql_table_name: ecomm.events ;;

  dimension: event_id {
    description: "Unique identifier for each website event (page view, action, etc.)"
    primary_key: yes
    type: number
    sql: ${TABLE}.id ;;
  }

  dimension: session_id {
    description: "Identifier grouping all events that occurred within the same user session"
    type: string
    sql: ${TABLE}.session_id ;;
  }

  dimension: utm_code {
    description: "Combined UTM tracking code constructed from ad event ID and referrer code, used to link website events to ad campaigns"
    type: string
    sql: ${ad_event_id}:: varchar || ' - ' || ${referrer_code} ::varchar ;;
  }

  dimension: ad_event_id {
    description: "Foreign key to the ad event that drove this website visit, if the event originated from an ad click"
    type: number
    sql: ${TABLE}.ad_event_id :: int ;;
  }

  dimension: referrer_code {
    description: "Referrer keyword ID used to match the event to a specific ad keyword"
    hidden: yes
    type: number
    sql: ${TABLE}.referrer_code :: int ;;
  }

  dimension: browser {
    description: "Web browser used by the visitor (e.g., Chrome, Safari, Firefox)"
    type: string
    sql: ${TABLE}.browser ;;
  }

  dimension: city {
    description: "City where the website event originated based on IP geolocation"
    type: string
    sql: ${TABLE}.city ;;
  }

  dimension: country {
    description: "Country where the website event originated based on IP geolocation"
    type: string
    map_layer_name: countries
    sql: ${TABLE}.country ;;
  }

  dimension_group: event {
    description: "Timestamp when the website event occurred"
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

  filter: previous_period_filter {
    type: date
    description: "Use this filter for period analysis"
  }

  dimension: previous_period {
    type: string
    description: "The reporting period as selected by the Previous Period Filter. Returns 'This Period' or 'Previous Period' to enable period-over-period comparison."
    sql:
      CASE
        WHEN {% date_start previous_period_filter %} is not null AND {% date_end previous_period_filter %} is not null /* date ranges or in the past x days */
          THEN
            CASE
              WHEN ${event_raw} >=  {% date_start previous_period_filter %}
                AND ${event_raw}  <= {% date_end previous_period_filter %}
                THEN 'This Period'
              WHEN ${event_raw}  >= DATEADD(day,-1*DATEDIFF('day',{% date_start previous_period_filter %}, {% date_end previous_period_filter %} ) + 1, DATEADD(day,-1,{% date_start previous_period_filter %} ) )
                AND ${event_raw}  <= DATEADD(day,-1,{% date_start previous_period_filter %} )
                THEN 'Previous Period'
            END
          END ;;
  }


  dimension: event_type {
    description: "Type of website event representing the user action (e.g., Home, Category, Brand, Product, Cart, Purchase, Login)"
    type: string
    sql: ${TABLE}.event_type ;;
  }

  dimension: ip_address {
    description: "IP address of the visitor, used for geolocation and session identification"
    type: string
    sql: ${TABLE}.ip_address ;;
  }

  dimension: latitude {
    description: "Geographic latitude coordinate of the visitor based on IP geolocation"
    type: number
    sql: ${TABLE}.latitude ;;
  }

  dimension: longitude {
    description: "Geographic longitude coordinate of the visitor based on IP geolocation"
    type: number
    sql: ${TABLE}.longitude ;;
  }

  dimension: os {
    description: "Operating system of the visitor's device (e.g., Windows, macOS, iOS, Android)"
    type: string
    sql: ${TABLE}.os ;;
  }

  dimension: sequence_number {
    description: "The ordinal position of this event within its session (1 = first event, i.e., landing page)"
    type: number
    sql: ${TABLE}.sequence_number ;;
  }

  dimension: state {
    description: "US state where the website event originated based on IP geolocation"
    type: string
    sql: ${TABLE}.state ;;
  }

  dimension: traffic_source {
    description: "The channel that brought the visitor to the site (e.g., Organic, Adwords, Email, Facebook)"
    type: string
    sql: ${TABLE}.traffic_source ;;
  }

  dimension: uri {
    description: "The page URI/path visited during this event"
    type: string
    sql: ${TABLE}.uri ;;
  }

  dimension: user_id {
    description: "Foreign key to the registered user who performed this event, null for anonymous visitors"
    type: number
    sql: ${TABLE}.user_id ;;
  }

  dimension: zip {
    description: "ZIP/postal code of the visitor based on IP geolocation"
    type: zipcode
    sql: ${TABLE}.zip ;;
  }

  dimension: is_entry_event {
    type: yesno
    description: "Yes indicates this was the entry point / landing page of the session"
    sql: ${sequence_number} = 1 ;;
  }

  dimension: is_exit_event {
    type: yesno
    label: "UTM Source"
    sql: ${sequence_number} = ${sessions.number_of_events_in_session} ;;
    description: "Yes indicates this was the exit point / bounce page of the session"
  }

  measure: count_bounces {
    type: count
    description: "Count of events where those events were the bounce page for the session"

    filters: {
      field: is_exit_event
      value: "Yes"
    }
  }

  measure: bounce_rate {
    type: number
    value_format_name: percent_2
    description: "Percent of events where those events were the bounce page for the session, out of all events"
    sql: ${count_bounces}*1.0 / nullif(${count}*1.0,0) ;;
  }

  dimension: full_page_url {
    description: "Full page URL path visited during this event, used to identify specific pages"
    sql: ${TABLE}.uri ;;
  }

  dimension: viewed_product_id {
    description: "Product ID extracted from the page URL when the event type is 'Product', identifying which product was viewed"
    type: number
    sql: CASE
        WHEN ${event_type} = 'Product' THEN right(${full_page_url},length(${full_page_url})-9)
      END
       ;;
  }

##### Funnel Analysis #####

  dimension: funnel_step {
    description: "Maps each event type to a numbered funnel stage: (1) Land, (2) Browse Inventory, (3) View Product, (4) Add Item to Cart, (5) Purchase"
    sql: CASE
        WHEN ${event_type} IN ('Login', 'Home') THEN '(1) Land'
        WHEN ${event_type} IN ('Category', 'Brand') THEN '(2) Browse Inventory'
        WHEN ${event_type} = 'Product' THEN '(3) View Product'
        WHEN ${event_type} = 'Cart' THEN '(4) Add Item to Cart'
        WHEN ${event_type} = 'Purchase' THEN '(5) Purchase'
      END
       ;;
  }

  dimension: funnel_step_adwords {
    description: "Funnel step for Adwords-attributed events only: (1) Land through (5) Purchase. Null for events without a UTM code."
    sql: CASE
        WHEN ${event_type} IN ('Login', 'Home') and ${utm_code} is [not] null THEN '(1) Land'
        WHEN ${event_type} IN ('Category', 'Brand') and ${utm_code} is [not] null THEN '(2) Browse Inventory'
        WHEN ${event_type} = 'Product' and ${utm_code} is [not] null THEN '(3) View Product'
        WHEN ${event_type} = 'Cart' and ${utm_code} is [not] null THEN '(4) Add Item to Cart'
        WHEN ${event_type} = 'Purchase' and ${utm_code} is [not] null THEN '(5) Purchase'
      END
       ;;
  }

#   measure: unique_visitors {
#     type: count_distinct
#     description: "Uniqueness determined by IP Address and User Login"
#     view_label: "Visitors"
#     sql: ${ip} ;;
#     drill_fields: [visitors*]
#   }

  dimension: location {
    description: "Geographic coordinates (latitude, longitude) of the visitor for map visualizations"
    type: location
    view_label: "Visitors"
    sql_latitude: ${TABLE}.latitude ;;
    sql_longitude: ${TABLE}.longitude ;;
  }

  dimension: approx_location {
    description: "Approximate geographic coordinates rounded to 1 decimal place for privacy-safe map visualizations"
    type: location
    view_label: "Visitors"
    sql_latitude: round(${TABLE}.latitude,1) ;;
    sql_longitude: round(${TABLE}.longitude,1) ;;
  }

#   dimension: has_user_id {
#     type: yesno
#     view_label: "Visitors"
#     description: "Did the visitor sign in as a website user?"
#     sql: ${users.id} > 0 ;;
#   }

  measure: count {
    description: "Total number of website events (page views and actions)"
    type: count
    drill_fields: [simple_page_info*]
  }

  measure: sessions_count {
    description: "Count of distinct sessions across all events"
    type: count_distinct
    sql: ${session_id} ;;
  }

  measure: count_m {
    description: "Total event count expressed in millions for high-level reporting"
    label: "Count (MM)"
    type: number
    hidden: yes
    sql: ${count}/1000000.0 ;;
    drill_fields: [simple_page_info*]
    value_format: "#.### \"M\""
  }

#   measure: unique_visitors_m {
#     label: "Unique Visitors (MM)"
#     view_label: "Visitors"
#     type: number
#     sql: count (distinct ${ip}) / 1000000.0 ;;
#     description: "Uniqueness determined by IP Address and User Login"
#     value_format: "#.### \"M\""
#     hidden: yes
#     drill_fields: [visitors*]
#   }
#
#   measure: unique_visitors_k {
#     label: "Unique Visitors (k)"
#     view_label: "Visitors"
#     type: number
#     hidden: yes
#     description: "Uniqueness determined by IP Address and User Login"
#     sql: count (distinct ${ip}) / 1000.0 ;;
#     value_format: "#.### \"k\""
#     drill_fields: [visitors*]
#   }

  set: simple_page_info {
    fields: [
      event_id,
      event_time,
      event_type,
      #       - os
      #       - browser
      full_page_url, user_id, funnel_step]
  }

  set: visitors {
    fields: [os, browser, user_id, count]
  }
}
