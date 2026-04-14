view: users {
  sql_table_name: ecomm.users ;;
  ## Demographics ##

  dimension: id {
    description: "Unique identifier for each registered user"
    primary_key: yes
    type: number
    sql: ${TABLE}.id ;;
    tags: ["user_id"]
  }

  dimension: first_name {
    description: "User's first name, title-cased"
    hidden: yes
    sql: INITCAP(${TABLE}.first_name) ;;
  }

  dimension: last_name {
    description: "User's last name, title-cased"
    hidden: yes
    sql: INITCAP(${TABLE}.last_name) ;;
  }

  dimension: name {
    description: "User's full name (first and last name combined)"
    sql: ${first_name} || ' ' || ${last_name} ;;
  }

  dimension: age {
    description: "User's age in years"
    type: number
    sql: ${TABLE}.age ;;
  }

  dimension: age_tier {
    description: "User's age grouped into decade-based tiers (0-9, 10-19, 20-29, etc.) for demographic analysis"
    type: tier
    tiers: [0, 10, 20, 30, 40, 50, 60, 70]
    style: integer
    sql: ${age} ;;
  }

  dimension: gender {
    description: "User's gender (e.g., Male, Female)"
    sql: ${TABLE}.gender ;;
  }

  dimension: gender_short {
    description: "Single-character lowercase gender abbreviation (e.g., 'm' or 'f'), used for image URL construction"
    sql: LOWER(LEFT(${gender},1)) ;;
  }

  dimension: user_image {
    description: "Profile image for the user, rendered as an HTML image tag"
    sql: ${image_file} ;;
    html: <img src="{{ value }}" width="220" height="220"/>;;
  }

  dimension: email {
    description: "User's email address, with links to the User Lookup Dashboard and email promotion action"
    sql: ${TABLE}.email ;;
    tags: ["email"]

    link: {
      label: "User Lookup Dashboard"
      url: "http://demo.looker.com/dashboards/160?Email={{ value | encode_uri }}"
      icon_url: "http://www.looker.com/favicon.ico"
    }
    action: {
      label: "Email Promotion to Customer"
      url: "https://desolate-refuge-53336.herokuapp.com/posts"
      icon_url: "https://sendgrid.com/favicon.ico"
      param: {
        name: "some_auth_code"
        value: "abc123456"
      }
      form_param: {
        name: "Subject"
        required: yes
        default: "Thank you {{ users.name._value }}"
      }
      form_param: {
        name: "Body"
        type: textarea
        required: yes
        default:
        "Dear {{ users.name._value }},

        Thanks for your loyalty to the Look.  We'd like to offer you a 10% discount
        on your next purchase!  Just use the code LOYAL when checking out!

        Your friends at the Look"
      }
    }
    required_fields: [name]
  }

  dimension: image_file {
    description: "URL path to the user's profile image based on gender"
    hidden: yes
    sql: ('https://docs.looker.com/assets/images/'||${gender_short}||'.jpg') ;;
  }

  ## Demographics ##

  dimension: city {
    description: "City where the user is located"
    sql: ${TABLE}.city ;;
    drill_fields: [zip]
  }

  dimension: state {
    description: "US state where the user is located"
    sql: ${TABLE}.state ;;
    map_layer_name: us_states
    drill_fields: [zip, city]
  }

  dimension: zip {
    description: "ZIP/postal code of the user's location"
    type: zipcode
    sql: ${TABLE}.zip ;;
  }

  dimension: uk_postcode {
    description: "UK postcode area extracted from the ZIP field for UK-based users, used for UK map visualizations"
    label: "UK Postcode"
    sql: CASE WHEN ${TABLE}.country = 'UK' THEN TRANSLATE(LEFT(${zip},2),'0123456789','') END ;;
    map_layer_name: uk_postcode_areas
    drill_fields: [city, zip]
  }

  dimension: country {
    description: "Country where the user is located, with 'UK' expanded to 'United Kingdom'"
    map_layer_name: countries
    drill_fields: [state, city]
    sql: CASE WHEN ${TABLE}.country = 'UK' THEN 'United Kingdom'
           ELSE ${TABLE}.country
           END
       ;;
  }

  dimension: location {
    description: "Precise geographic coordinates (latitude, longitude) of the user for map visualizations"
    type: location
    sql_latitude: ${TABLE}.latitude ;;
    sql_longitude: ${TABLE}.longitude ;;
  }

  dimension: approx_location {
    description: "Approximate geographic coordinates rounded to 1 decimal place for privacy-safe map visualizations"
    type: location
    drill_fields: [location]
    sql_latitude: round(${TABLE}.latitude,1) ;;
    sql_longitude: round(${TABLE}.longitude,1) ;;
  }

  ## Other User Information ##

  dimension_group: created {
    description: "Timestamp when the user account was created/registered"
    type: time
    timeframes: [time, date, month, raw]
    sql: ${TABLE}.created_at ;;
  }

  dimension: history {
    description: "Link to the user's order history in the order_items explore"
    sql: ${TABLE}.id ;;
    html: <a href="/explore/thelook/order_items?fields=order_items.detail*&f[users.id]={{ value }}">Order History</a>
      ;;
  }

  dimension: traffic_source {
    description: "The marketing channel through which the user originally registered (e.g., Organic, Adwords, Email)"
    sql: ${TABLE}.traffic_source ;;
  }

  dimension: ssn {
    # dummy field used in next dim
    hidden: yes
    type: number
    sql: lpad(cast(round(random() * 10000, 0) as char(4)), 4, '0') ;;
  }

  dimension: ssn_last_4 {
    label: "SSN Last 4"
    description: "Only users with sufficient permissions will see this data"
    type: string
    sql:
          CASE  WHEN '{{_user_attributes["can_see_sensitive_data"]}}' = 'yes'
                THEN ${ssn}
                ELSE MD5(${ssn}||'salt')
          END;;
    html:
          {% if _user_attributes["can_see_sensitive_data"]  == 'yes' %}
          {{ value }}
          {% else %}
            ####
          {% endif %}  ;;
  }

  ## MEASURES ##

  measure: count {
    description: "Total number of registered users"
    type: count
    drill_fields: [detail*]
  }

  measure: count_percent_of_total {
    description: "Each group's user count as a percentage of the total user count, useful for demographic breakdowns"
    label: "Count (Percent of Total)"
    type: percent_of_total
    sql: ${count} ;;
    drill_fields: [detail*]
  }

  measure: average_age {
    description: "Average age of users in years"
    type: average
    value_format_name: decimal_2
    sql: ${age} ;;
    drill_fields: [detail*]
  }

  set: detail {
    fields: [id, name, email, age, created_date, created_month, orders.count, order_items.count]
  }

  set: user_facts {
    fields: [name, email, age, gender, created_date, created_month,
             count, average_age, count_percent_of_total]
  }
}
