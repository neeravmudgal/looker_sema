view: keywords {
  sql_table_name: ecomm.keywords ;;

  dimension: keyword_id {
    description: "Unique identifier for each keyword used in ad targeting"
    primary_key: yes
    type: number
    sql: ${TABLE}.KEYWORD_ID ;;
  }

  dimension: ad_id {
    description: "Foreign key to the ad group this keyword belongs to"
    type: number
    sql: ${TABLE}.AD_ID ;;
  }

  dimension: bidding_strategy_type {
    description: "The automated bidding strategy applied to this keyword (e.g., Manual CPC, Target CPA)"
    type: string
    sql: ${TABLE}.BIDDING_STRATEGY_TYPE ;;
  }

  dimension: cpc_bid_amount {
    description: "Maximum cost-per-click bid amount in USD for this keyword"
    label: "CPC Bid (USD)"
    type: number
    sql:( ${TABLE}.CPC_BID_AMOUNT * 1.0 )/100.0 ;;
    value_format_name: usd
  }

  dimension_group: created {
    description: "Date when the keyword was added to the ad group"
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

  dimension: criterion_name {
    description: "The actual keyword text or phrase used for ad targeting, with links to Google Search and AdWords"
    type: string
    sql: ${TABLE}.CRITERION_NAME ;;
    link: {
      icon_url: "https://www.google.com/images/branding/product/ico/googleg_lodp.ico"
      label: "Google Search"
      url: "https://www.google.com/search?q={{ value | encode_uri}}"
    }
    link: {
      label: "View on AdWords"
      icon_url: "https://www.google.com/s2/favicons?domain=www.adwords.google.com"
      url: "https://adwords.google.com/aw/adgroups?campaignId={{ criterion_name._value | encode_uri }}"
    }
    link: {
      label: "Pause Keyword"
      icon_url: "https://www.google.com/s2/favicons?domain=www.adwords.google.com"
      url: "https://adwords.google.com/aw/ads?keywordId={{ criterion_name._value | encode_uri }}"
    }
  }

  dimension: keyword_match_type {
    description: "The match type setting for the keyword (e.g., Broad, Phrase, Exact) controlling which search queries trigger the ad"
    type: string
    sql: ${TABLE}.KEYWORD_MATCH_TYPE ;;
  }

  dimension: period {
    description: "Duration period in days for the keyword's active run"
    type: number
    sql: ${TABLE}.PERIOD ;;
  }

  dimension: quality_score {
    description: "Google Ads quality score (1-10) rating the relevance and quality of the keyword and its associated ad"
    type: number
    sql: ${TABLE}.QUALITY_SCORE ;;
  }

  dimension: system_serving_status {
    description: "Current serving status of the keyword in the ad system (e.g., eligible, rarely shown, disapproved)"
    type: string
    sql: ${TABLE}.SYSTEM_SERVING_STATUS ;;
  }

  measure: count {
    description: "Total number of keywords"
    type: count
    drill_fields: [keyword_id, criterion_name, adevents.count]
  }
}
