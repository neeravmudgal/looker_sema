view: campaigns {

derived_table: {
  datagroup_trigger: ecommerce_etl
  sql: SELECT *
      FROM   ecomm.campaigns
      UNION
      SELECT 9999                 AS id,
      NULL                        AS advertising_channel,
      0                           AS amount,
      NULL                        AS bid_type,
      'Total'                     AS campaign_name,
      '60'                        AS period,
      Dateadd(day, -1, current_timestamp()::timestamp_ntz) AS created_at  ;;
}

##### Campaign Facts #####

  filter: campaign_selector {
    description: "Filter to select a specific campaign for comparison against the benchmark of all other campaigns"
    type: string
    suggest_dimension: campaign_name
  }

  dimension: campaign_benchmark {
    description: "Groups the selected campaign vs. all others as 'Benchmark' for comparative analysis"
    type: string
    sql: case when ( {% condition campaign_selector %} ${campaign_name} {% endcondition %}) then ${campaign_name} else 'Benchmark' end  ;;
  }

  dimension: campaign_id {
    description: "Unique identifier for each advertising campaign"
    primary_key: yes
    type: number
    sql: ${TABLE}.id ;;
  }

  dimension: advertising_channel {
    description: "The advertising channel type (e.g., Search, Display) indicating how the campaign serves ads"
    type: string
    sql: ${TABLE}.advertising_channel ;;
  }

  dimension: amount {
    description: "The budget amount allocated to the campaign in raw units (cents)"
    type: number
    sql: ${TABLE}.amount ;;
  }

  dimension: bid_type {
    description: "The bidding strategy used for the campaign (e.g., CPC, CPM, CPA)"
    type: string
    sql: ${TABLE}.bid_type ;;
  }

  dimension: campaign_name {
    description: "Full campaign name including ID prefix, with links to performance dashboard and AdWords"
    full_suggestions: yes
    type: string
    sql: ${campaign_id}::VARCHAR ||  ' - ' || ${campaign_name_raw} ;;
    link: {
      label: "Campaign Performance Dashboard"
      icon_url: "http://www.looker.com/favicon.ico"
      url: "https://demo.looker.com/dashboards/3106?Campaign Name={{ value | encode_uri }}"
    }
    link: {
      label: "View on AdWords"
      icon_url: "https://www.google.com/s2/favicons?domain=www.adwords.google.com"
      url: "https://adwords.google.com/aw/adgroups?campaignId={{ campaign_id._value | encode_uri }}"
    }
    link: {
      label: "Pause Campaign"
      icon_url: "https://www.google.com/s2/favicons?domain=www.adwords.google.com"
      url: "https://adwords.google.com/aw/ads?campaignId={{ campaign_id._value | encode_uri }}"
    }
  }

  dimension: campaign_name_raw {
    description: "Abbreviated campaign name as stored in the source data, without the ID prefix"
    label: "Campaign Abbreviated"
    sql: ${TABLE}.campaign_name ;;
    link: {
      label: "Campaign Performance Dashboard"
      icon_url: "http://www.looker.com/favicon.ico"
      url: "https://demo.looker.com/dashboards/3106?Campaign Name={{ campaign_name._value | encode_uri }}"
    }
    link: {
      label: "View on AdWords"
      icon_url: "https://www.google.com/s2/favicons?domain=www.adwords.google.com"
      url: "https://adwords.google.com/aw/adgroups?campaignId={{ campaign_id._value | encode_uri }}"
    }
    link: {
      label: "Pause Campaign"
      icon_url: "https://www.google.com/s2/favicons?domain=www.adwords.google.com"
      url: "https://adwords.google.com/aw/ads?campaignId={{ campaign_id._value | encode_uri }}"
    }
  }

  dimension: campaign_type {
    description: "The sub-type or category of the campaign, extracted from the campaign name string"
    sql: substring(substring(${campaign_name_raw},POSITION(' - ', ${campaign_name_raw})+3),POSITION(' - ', substring(${campaign_name_raw},POSITION(' - ', ${campaign_name_raw})+3))+3) ;;
  }

  dimension_group: created {
    description: "Date when the campaign was created and started running"
    type: time
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year
    ]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.CREATED_AT ;;
  }

  dimension_group: end {
    description: "Calculated end date of the campaign based on creation date plus the campaign period duration"
    type: time
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year
    ]
    convert_tz: no
    datatype: date
    sql: dateadd('day', ${period},${created_date}) ;;
  }

  dimension: day_of_quarter {
    description: "Number of days elapsed since the start of the campaign's creation quarter"
    type: number
    sql: DATEDIFF(
        'day',
        CAST(CONCAT(${created_quarter}, '-01') as date),
        ${created_raw})
       ;;
  }

  dimension: period {
    description: "Campaign duration in days — how long the campaign is configured to run"
    type: number
    sql: ${TABLE}.period :: int ;;
  }

  dimension: is_active_now {
    description: "Yes if the campaign end date is today or in the future, indicating it is still active"
    type: yesno
    sql: ${end_date} >= CURRENT_DATE ;;
  }

  measure: count {
    description: "Total number of campaigns"
    type: count
    drill_fields: [campaign_id, campaign_name, adgroups.count]
  }

  set: detail {
    fields: [
      campaign_id, campaign_name, adgroups.count
    ]
  }
}
