explore: user_session_fact {
  hidden: yes
}


view: user_session_fact {
  derived_table: {
    datagroup_trigger: ecommerce_etl
    explore_source: events {
      column: session_user_id { field: sessions.session_user_id }
      column: site_acquisition_ad_event_id { field: sessions.site_acquisition_ad_event_id }
      column: site_acquisition_source { field: sessions.site_acquisition_source }
      column: first_visit_dt { field: sessions.first_visit_dt }
      column: first_purchase_dt { field: sessions.first_purchase_dt }
      column: session_count { field: sessions.count }
      column: count_bounce_sessions { field: sessions.count_bounce_sessions }
      column: count_with_cart { field: sessions.count_with_cart }
      column: count_with_purchase { field: sessions.count_with_purchase }
    }
  }
  dimension: session_user_id {
    description: "Unique user identifier — primary key linking aggregated session facts to the user"
    primary_key: yes
  }

  dimension: site_acquisition_ad_event_id {
    description: "Ad event ID from the user's first-ever session, identifying which ad originally brought them to the site"
    type: number
    sql: ${TABLE}.site_acquisition_ad_event_id :: int ;;
  }
  dimension: site_acquisition_source {
    description: "Traffic source of the user's first-ever session (e.g., Adwords, Organic, Email), representing how they were originally acquired"
    type: string
  }
  dimension_group: first_visit {
    description: "Date of the user's first-ever website visit"
    type: time
    timeframes:
      [
      raw
      ,date
      ,week
      ,month
      ,quarter
      ,year
      ,day_of_week
      ,day_of_month
      ,week_of_year
      ,month_num
      ,quarter_of_year
      ]
      sql: ${TABLE}.first_visit_dt ;;
  }

  dimension_group: first_purchase {
    description: "Date of the user's first-ever purchase on the site"
    type: time
    timeframes:
    [
      raw
      ,date
      ,week
      ,month
      ,quarter
      ,year
      ,day_of_week
      ,day_of_month
      ,week_of_year
      ,month_num
      ,quarter_of_year
    ]
    sql: ${TABLE}.first_purchase_dt ;;
  }
  dimension: session_count {
    description: "Total number of sessions the user has had across their entire lifetime on the site"
    label: "Lifetime Sessions"
    type: number
    drill_fields: [first_visit_month]
  }
  dimension: session_count_tier {
    description: "Lifetime session count grouped into tiers: 0-2, 3-5, 6-8, 9+ for segmentation analysis"
    label: "Lifetime Sessions Tier"
    type: tier
    sql: ${session_count} ;;
    tiers: [0,3,6,9]
    style: integer
  }

  dimension: count_bounce_sessions {
    description: "Total number of the user's sessions that were bounces (single-page visits)"
    type: number
  }
  dimension: count_with_cart {
    description: "Total number of the user's sessions that included at least one add-to-cart event"
    type: number
  }
  dimension: count_with_purchase {
    description: "Total number of the user's sessions that resulted in a purchase"
    type: number
    drill_fields: [first_visit_month]
  }
  dimension: count_with_purchase_tier {
    description: "Lifetime purchase count grouped into tiers: None (0), 1-2, or 3+ purchases for customer segmentation"
    label: "Lifetime Purchases Tier"
    type: string
    case: {
      when: {
        sql: ${count_with_purchase} = 0 ;;
        label: "None"
      }

      when: {
        sql: ${count_with_purchase} > 0 AND ${count_with_purchase} < 3 ;;
        label: "1-2"
      }

      when: {
        sql:${count_with_purchase} >= 3 ;;
        label: "3+"
      }
    }

    sql: ${count_with_purchase} ;;

  }

  dimension: has_purchase {
    description: "Yes if the user has made at least one purchase, indicating they are a converted customer"
    label: "Is Customer (Y/N)"
    type: yesno
    sql: ${count_with_purchase}>0 ;;
  }

  measure: count {
    description: "Total number of users"
    type: count
    drill_fields: [campaigns.advertising_channel,count_p1]
  }

  measure: count_p1 {
    description: "User count with drill-down to campaign type level"
    label: "Count"
    hidden: yes
    type: count
    drill_fields: [campaigns.campaign_type,count_p2]
  }
  measure: count_p2 {
    description: "User count with drill-down to campaign name level"
    label: "Count"
    hidden: yes
    type: count
    drill_fields: [campaigns.campaign_name_raw,count_p3]
  }
  measure: count_p3 {
    description: "User count with drill-down to keyword level"
    label: "Count"
    hidden: yes
    type: count
    drill_fields: [keywords.criterion_name,count_p4]
  }
  measure: count_p4 {
    description: "User count at the deepest drill level"
    label: "Count"
    hidden: yes
    type: count
  }
  measure: average_loyalty {
    description: "Average number of purchase sessions per user, indicating customer loyalty and repeat purchase behavior"
    type: average
    value_format_name: decimal_1
    sql: ${count_with_purchase} ;;
  }
  measure: average_engagement {
    description: "Average number of sessions per user, indicating overall site engagement level"
    type: average
    value_format_name: decimal_1
    sql: ${session_count} ;;
  }

  dimension: preferred_category {
    description: "Simulated preferred product category based on the user's acquisition source, used for cohort analysis"
    sql:
      CASE
      WHEN ${site_acquisition_source} = 'Adwords'
        THEN
            CASE
              WHEN random() <.6 THEN 'Jeans'
              WHEN random() <.7 THEN 'Accessories'
              ELSE 'Tops'
            END

      WHEN ${site_acquisition_source} = 'Email'
        THEN
            CASE
              WHEN random() <.05 THEN 'Jeans'
              WHEN random() <.4 THEN 'Accessories'
              ELSE 'Tops'
            END

       WHEN ${site_acquisition_source} = 'Facebook'
        THEN
            CASE
              WHEN random() <.6 THEN 'Jeans'
              WHEN random() <.7 THEN 'Accessories'
              ELSE 'Tops'
            END
      WHEN ${site_acquisition_source} = 'Organic'
        THEN
            CASE
              WHEN random() <.6 THEN 'Jeans'
              WHEN random() <.7 THEN 'Accessories'
              ELSE 'Tops'
            END

      WHEN ${site_acquisition_source} = 'Youtube'
      THEN
          CASE
            WHEN random() <.6 THEN 'Jeans'
            WHEN random() <.7 THEN 'Accessories'
            ELSE 'Tops'
          END
      ELSE
            CASE
              WHEN random() <.6 THEN 'Jeans'
              WHEN random() <.7 THEN 'Accessories'
              ELSE 'Tops'
            END
      END
    ;;


  }

  set: user_session_measures {
    fields: [
      first_visit_month,
      average_engagement,
      average_loyalty
    ]
  }
}
