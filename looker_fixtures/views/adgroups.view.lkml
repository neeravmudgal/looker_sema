view: adgroups {
  view_label: "Ad Groups"
  sql_table_name: ecomm.ad_groups ;;

  dimension: ad_id {
    description: "Unique identifier for the ad group"
    primary_key: yes
    type: number
    sql: ${TABLE}.ad_id ;;
  }

  dimension: ad_type {
    description: "The format or type of ad in this group (e.g., text, display, video)"
    type: string
    sql: ${TABLE}.ad_type ;;
  }

  dimension: campaign_id {
    description: "Foreign key to the parent campaign this ad group belongs to"
    type: number
    hidden: yes
    sql: ${TABLE}.campaign_id ;;
  }

  dimension_group: created {
    description: "Date when the ad group was created"
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
    sql: ${TABLE}.created_at ;;
  }

  dimension: headline {
    description: "The headline text displayed in the ad creative"
    type: string
    sql: ${TABLE}.headline ;;
  }

  dimension: name {
    description: "Name of the ad group, with links to view, pause, or change bid in AdWords"
    link: {
      label: "View on AdWords"
      icon_url: "https://www.google.com/s2/favicons?domain=www.adwords.google.com"
      url: "https://adwords.google.com/aw/ads?campaignId={{ campaign_id._value }}&adGroupId={{ ad_id._value }}"
    }
    link: {
      label: "Pause Ad Group"
      icon_url: "https://www.google.com/s2/favicons?domain=www.adwords.google.com"
      url: "https://adwords.google.com/aw/ads?campaignId={{ campaign_id._value }}&adGroupId={{ ad_id._value }}"
    }
    link: {
      url: "https://adwords.google.com/aw/ads?campaignId={{ campaign_id._value }}&adGroupId={{ ad_id._value }}"
      icon_url: "https://www.gstatic.com/awn/awsm/brt/awn_awsm_20171108_RC00/aw_blend/favicon.ico"
      label: "Change Bid"
    }
    type: string
    sql: ${TABLE}.name ;;
  }

  dimension: period {
    description: "Duration period of the ad group in days"
    type: number
    sql: ${TABLE}.period ;;
  }

  measure: count {
    description: "Total number of ad groups"
    type: count
    drill_fields: [campaigns.campaign_name, name, ad_type, created_date]
  }
}
