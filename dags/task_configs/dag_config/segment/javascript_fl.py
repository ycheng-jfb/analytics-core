from snowflake.snowpark.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    ArrayType,
    BooleanType,
    MapType,
    LongType,
    DoubleType,
)

javascript_fl_schema = StructType(
    [
        StructField("anonymousId", StringType(), True),
        StructField("category", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("UUID_TS", StringType(), True),
        StructField(
            "context",
            StructType(
                [
                    StructField(
                        "campaign",
                        StructType(
                            [
                                StructField(
                                    "3bamp_3bamp_3butm_content", StringType(), True
                                ),
                                StructField(
                                    "3bamp_3bamp_3butm_term", StringType(), True
                                ),
                                StructField("3bamp_3butm_campaign", StringType(), True),
                                StructField("3bamp_3butm_content", StringType(), True),
                                StructField("3bamp_3butm_medium", StringType(), True),
                                StructField("3bamp_3butm_source", StringType(), True),
                                StructField("3bamp_3butm_term", StringType(), True),
                                StructField("3butm_campaign", StringType(), True),
                                StructField("3butm_content", StringType(), True),
                                StructField("3butm_medium", StringType(), True),
                                StructField("3butm_source", StringType(), True),
                                StructField("3butm_term", StringType(), True),
                                StructField("amp_3butm_content", StringType(), True),
                                StructField("amp_3butm_term", StringType(), True),
                                StructField(
                                    "amp_amp_amp_utm_content", StringType(), True
                                ),
                                StructField("amp_amp_amp_utm_term", StringType(), True),
                                StructField("amp_amp_utm_content", StringType(), True),
                                StructField("amp_amp_utm_term", StringType(), True),
                                StructField("amp_utm_campaign", StringType(), True),
                                StructField("amp_utm_content", StringType(), True),
                                StructField("amp_utm_medium", StringType(), True),
                                StructField("amp_utm_source", StringType(), True),
                                StructField("amp_utm_term", StringType(), True),
                                StructField(
                                    "campamember_20services_20fabletics_20numbergn",
                                    StringType(),
                                    True,
                                ),
                                StructField("content", StringType(), True),
                                StructField("f", StringType(), True),
                                StructField("faceedium", StringType(), True),
                                StructField("large", StringType(), True),
                                StructField("medifableticsm", StringType(), True),
                                StructField("medium", StringType(), True),
                                StructField(
                                    "mefabletics_20logindium", StringType(), True
                                ),
                                StructField("m_medium", StringType(), True),
                                StructField("name", StringType(), True),
                                StructField("nooverride", StringType(), True),
                                StructField("sfabletics_comource", StringType(), True),
                                StructField("small", StringType(), True),
                                StructField("source", StringType(), True),
                                StructField("sourcev", StringType(), True),
                                StructField(
                                    "s_jackie_20square_20neck_20strap_20tie_20jumpsuit_20_20francesca_27surce",
                                    StringType(),
                                    True,
                                ),
                                StructField("term", StringType(), True),
                                StructField("tm_source", StringType(), True),
                                StructField("utm_campaign", StringType(), True),
                                StructField("utm_content", StringType(), True),
                                StructField("utm_medium", StringType(), True),
                                StructField("utm_source", StringType(), True),
                                StructField("utm_term", StringType(), True),
                                StructField(
                                    "medium%3Dpaid_social_media", StringType(), True
                                ),
                            ]
                        ),
                    ),
                    StructField("ip", StringType(), True),
                    StructField(
                        "library",
                        StructType(
                            [
                                StructField("name", StringType(), True),
                                StructField("version", StringType(), True),
                            ]
                        ),
                    ),
                    StructField("locale", StringType(), True),
                    StructField(
                        "page",
                        StructType(
                            [
                                StructField("path", StringType(), True),
                                StructField("referrer", StringType(), True),
                                StructField("search", StringType(), True),
                                StructField("title", StringType(), True),
                                StructField("url", StringType(), True),
                            ]
                        ),
                    ),
                    StructField(
                        "protocols",
                        StructType(
                            [
                                StructField("sourceId", StringType(), True),
                                StructField(
                                    "violations",
                                    ArrayType(
                                        StructType(
                                            [
                                                StructField(
                                                    "description", StringType(), True
                                                ),
                                                StructField(
                                                    "field", StringType(), True
                                                ),
                                                StructField("type", StringType(), True),
                                            ]
                                        )
                                    ),
                                ),
                            ]
                        ),
                    ),
                    StructField("userAgent", StringType(), True),
                ]
            ),
        ),
        StructField("event", StringType(), True),
        StructField("messageId", StringType(), True),
        StructField("name", StringType(), True),
        StructField("originalTimestamp", StringType(), True),
        StructField("projectId", StringType(), True),
        StructField(
            "properties",
            StructType(
                [
                    StructField("product_tag", StringType(), True),
                    StructField("is_vip", BooleanType(), True),
                    StructField("test_variation_id", StringType(), True),
                    StructField("test_variation_name", StringType(), True),
                    StructField("id", StringType(), True),
                    StructField("quiz_question", StringType(), True),
                    StructField(
                        "question_values",
                        StructType(
                            [
                                StructField("kids_interest", StringType(), True),
                                StructField("mens_interest", StringType(), True),
                                StructField("no_interest", StringType(), True),
                                StructField("scrubs_interest", StringType(), True),
                                StructField("shapewear_interest", StringType(), True),
                                StructField("golf_interest", StringType(), True),
                                StructField("tennis_interest", StringType(), True),
                                StructField("womens_interest", StringType(), True),
                                StructField("birth_month", StringType(), True),
                                StructField("birth_year", StringType(), True),
                                StructField("zip_code", StringType(), True),
                                StructField("size_bottom", StringType(), True),
                                StructField("size_top", StringType(), True),
                                StructField("size_bra", StringType(), True),
                                StructField(
                                    "referrer",
                                    MapType(StringType(), StringType(), True),
                                    True,
                                ),
                            ]
                        ),
                    ),
                    StructField("creative", StringType(), True),
                    StructField("fromMobileApp", BooleanType(), True),
                    StructField("utm_source", StringType(), True),
                    StructField("utm_medium", StringType(), True),
                    StructField("utm_campaign", StringType(), True),
                    StructField("action", StringType(), True),
                    StructField("cart_id", StringType(), True),
                    StructField("inventory_count", LongType(), True),
                    StructField("algo_id", StringType(), True),
                    StructField("algo_test_key", StringType(), True),
                    StructField("angle", StringType(), True),
                    StructField("automated_test", BooleanType(), True),
                    StructField("bundle_alias", StringType(), True),
                    StructField("bundle_index", LongType(), True),
                    StructField("bundle_name", StringType(), True),
                    StructField("bundle_product_id", StringType(), True),
                    StructField("bundle_product_id_hit", StringType(), True),
                    StructField("bundle_quantity", StringType(), True),
                    StructField("bundle_retail_price", StringType(), True),
                    StructField("bundle_sale_price", DoubleType(), True),
                    StructField("bundle_url", StringType(), True),
                    StructField("bundle_vip_price", StringType(), True),
                    StructField("category", StringType(), True),
                    StructField("customer_bucket_group", StringType(), True),
                    StructField("customer_gender", StringType(), True),
                    StructField("customer_id", StringType(), True),
                    StructField("dmg_code", StringType(), True),
                    StructField("esp_id", StringType(), True),
                    StructField("event", StringType(), True),
                    StructField("feature", StringType(), True),
                    StructField("fpl_id", LongType(), True),
                    StructField("gateway", BooleanType(), True),
                    StructField("gender_selected", StringType(), True),
                    StructField("global_codes", StringType(), True),
                    StructField("grid_label", StringType(), True),
                    StructField("has_reviews", BooleanType(), True),
                    StructField("image_url", StringType(), True),
                    StructField("inventory_availability", LongType(), True),
                    StructField("isQuickView", BooleanType(), True),
                    StructField("is_bundle", BooleanType(), True),
                    StructField("is_default_model", BooleanType(), True),
                    StructField("label", StringType(), True),
                    StructField("list", StringType(), True),
                    StructField("list_id", StringType(), True),
                    StructField("location", StringType(), True),
                    StructField("logged_in_status", BooleanType(), True),
                    StructField("loggedin_status", StringType(), True),
                    StructField("membership_brand_id", LongType(), True),
                    StructField("membership_level_group_id", LongType(), True),
                    StructField("model_height", StringType(), True),
                    StructField("model_id", StringType(), True),
                    StructField("model_name", StringType(), True),
                    StructField("name", StringType(), True),
                    StructField("opt_in", BooleanType(), True),
                    StructField("page_hostname", StringType(), True),
                    StructField("page_name", StringType(), True),
                    StructField("path", StringType(), True),
                    StructField("position", StringType(), True),
                    StructField("price", DoubleType(), True),
                    StructField("product_display_reason", StringType(), True),
                    StructField("product_id", StringType(), True),
                    StructField("product_id_hit", ArrayType(StringType()), True),
                    StructField(
                        "products",
                        ArrayType(
                            StructType(
                                [
                                    StructField("product_tag", StringType(), True),
                                    StructField("bundle_alias", StringType(), True),
                                    StructField("bundle_name", StringType(), True),
                                    StructField(
                                        "bundle_product_id", StringType(), True
                                    ),
                                    StructField("bundle_quantity", StringType(), True),
                                    StructField(
                                        "bundle_retail_price", StringType(), True
                                    ),
                                    StructField(
                                        "bundle_sale_price", StringType(), True
                                    ),
                                    StructField("bundle_url", StringType(), True),
                                    StructField("bundle_vip_price", StringType(), True),
                                    StructField("category", StringType(), True),
                                    StructField("grid_label", StringType(), True),
                                    StructField("has_reviews", BooleanType(), True),
                                    StructField("image_url", StringType(), True),
                                    StructField(
                                        "inventory_availability", LongType(), True
                                    ),
                                    StructField("is_bundle", BooleanType(), True),
                                    StructField("name", StringType(), True),
                                    StructField("position", LongType(), True),
                                    StructField("price", DoubleType(), True),
                                    StructField(
                                        "product_display_reason", StringType(), True
                                    ),
                                    StructField("product_id", StringType(), True),
                                    StructField("psrc", StringType(), True),
                                    StructField("quantity", LongType(), True),
                                    StructField("retail_price", DoubleType(), True),
                                    StructField("review_num", LongType(), True),
                                    StructField("review_rating", DoubleType(), True),
                                    StructField("sale_price", DoubleType(), True),
                                    StructField("size", StringType(), True),
                                    StructField("sized_product_id", LongType(), True),
                                    StructField("sized_sku", StringType(), True),
                                    StructField("sku", StringType(), True),
                                    StructField("url", StringType(), True),
                                    StructField("variant", StringType(), True),
                                    StructField("vip_price", DoubleType(), True),
                                ]
                            )
                        ),
                    ),
                    StructField("psrc", StringType(), True),
                    StructField("quantity", LongType(), True),
                    StructField("query", StringType(), True),
                    StructField("referrer", StringType(), True),
                    StructField("referring_page_module", StringType(), True),
                    StructField("retail_price", DoubleType(), True),
                    StructField("review_num", LongType(), True),
                    StructField("review_rating", DoubleType(), True),
                    StructField("sale_price", DoubleType(), True),
                    StructField("search", StringType(), True),
                    StructField("session_id", StringType(), True),
                    StructField("sha_email", StringType(), True),
                    StructField("size", StringType(), True),
                    StructField("size_type", StringType(), True),
                    StructField("sized_product_id", StringType(), True),
                    StructField("sized_sku", StringType(), True),
                    StructField("sku", StringType(), True),
                    StructField("source", StringType(), True),
                    StructField("storeGroup", LongType(), True),
                    StructField("storeGroupId", LongType(), True),
                    StructField("store_group_id", StringType(), True),
                    StructField("store_id", LongType(), True),
                    StructField("title", StringType(), True),
                    StructField("type", StringType(), True),
                    StructField("url", StringType(), True),
                    StructField("user_status_initial", StringType(), True),
                    StructField("variant", StringType(), True),
                    StructField("vip_price", DoubleType(), True),
                    StructField("visitor_group", StringType(), True),
                    StructField("vistor_id", StringType(), True),
                ]
            ),
        ),
        StructField("receivedAt", StringType(), True),
        StructField("sentAt", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField(
            "traits",
            StructType(
                [
                    StructField("-----", StringType(), True),
                    StructField(
                        "Anniversaire_(facultatif_-_JJ/MM)", StringType(), True
                    ),
                    StructField("Birthday_(optional_-_dd/mm)", StringType(), True),
                    StructField("Birthday_(optional_-_mm/dd/yy)", StringType(), True),
                    StructField("Bottom_Size", StringType(), True),
                    StructField(
                        "Comment_avez-vous_entendu_parler_de_nous_?_",
                        StringType(),
                        True,
                    ),
                    StructField(
                        "De_quelle_offre_réservée_aux_nouveaux_membres_VIP_souhaitez-vous_profiter_?",
                        StringType(),
                        True,
                    ),
                    StructField("Geburtstag_(optional_-_TT/MM)", StringType(), True),
                    StructField("Hosengröße", StringType(), True),
                    StructField("How_did_you_hear_about_us?", StringType(), True),
                    StructField("Oberteil-Größe", StringType(), True),
                    StructField(
                        "Que_recherchez-vous_aujourd'hui_?", StringType(), True
                    ),
                    StructField(
                        "Quelle_couleur_de_short_porteriez-vous_le_plus_souvent_?",
                        StringType(),
                        True,
                    ),
                    StructField(
                        "Quelle_couleur_de_tee-shirt_porteriez-vous_le_plus_souvent_?",
                        StringType(),
                        True,
                    ),
                    StructField("Quelle_est_ta_taille_de_bas?", StringType(), True),
                    StructField(
                        "Quelle_est_votre_activité_préférée_?", StringType(), True
                    ),
                    StructField("Quelle_est_votre_taille_de_haut?", StringType(), True),
                    StructField(
                        "Quelle_propriété_technique_privilégiez-vous_pour_vos_vêtements_?",
                        StringType(),
                        True,
                    ),
                    StructField("Style_Auswahl:_was_shoppst_du?", StringType(), True),
                    StructField(
                        "Style_selector:_What_are_you_shopping_for?", StringType(), True
                    ),
                    StructField(
                        "Sélecteur_de_style_:_Que_cherchez-vous_?", StringType(), True
                    ),
                    StructField("Taille_de_bas", StringType(), True),
                    StructField("Taille_de_haut", StringType(), True),
                    StructField("Top_Size", StringType(), True),
                    StructField("Was_suchst_Du_genau?", StringType(), True),
                    StructField(
                        "Welche_Eigenschafen_sollte_Deine_Fitnessbekleidung_besitzen?",
                        StringType(),
                        True,
                    ),
                    StructField("Welche_Größe_trägst_Du?", StringType(), True),
                    StructField("Welche_Hosengröße_hast_Du?", StringType(), True),
                    StructField(
                        "Welche_Shorts-Farbe_würdest_Du_am_häufigsten_tragen?",
                        StringType(),
                        True,
                    ),
                    StructField(
                        "Welche_T-Shirt-Farbe_würdest_Du_am_häufigsten_tragen?",
                        StringType(),
                        True,
                    ),
                    StructField(
                        "Welches_Angebot_für_neue_VIPs_würdest_Du_gerne_nutzen?",
                        StringType(),
                        True,
                    ),
                    StructField(
                        "Welches_Workout_machst_Du_am_liebsten?", StringType(), True
                    ),
                    StructField("What's_your_favourite_workout?", StringType(), True),
                    StructField("What's_your_go-to_workout?", StringType(), True),
                    StructField("What_are_you_shopping_for_today?", StringType(), True),
                    StructField("What_is_your_bottoms_size?", StringType(), True),
                    StructField("What_is_your_shirt_size?", StringType(), True),
                    StructField("What_is_your_waist_size?", StringType(), True),
                    StructField(
                        "What_tech_do_you_need_the_MOST_in_your_gear?",
                        StringType(),
                        True,
                    ),
                    StructField(
                        "Which_New_VIP_Offer_do_you_prefer?", StringType(), True
                    ),
                    StructField("Which_New_VIP_Offer_do_you_want?", StringType(), True),
                    StructField("Which_Offer_do_you_want?", StringType(), True),
                    StructField(
                        "Which_color_shorts_would_you_wear_the_most?",
                        StringType(),
                        True,
                    ),
                    StructField(
                        "Which_color_tee_would_you_wear_the_most?", StringType(), True
                    ),
                    StructField(
                        "Which_colour_shorts_would_you_wear_the_most?",
                        StringType(),
                        True,
                    ),
                    StructField(
                        "Which_colour_tee_would_you_wear_the_most?", StringType(), True
                    ),
                    StructField("Wie_hast_Du_von_uns_erfahren?", StringType(), True),
                    StructField(
                        "Your_bottom_size_preference_(waist)", StringType(), True
                    ),
                    StructField("Your_top_size_preference_(chest)", StringType(), True),
                    StructField("Zip_Code*", StringType(), True),
                    StructField("activated_date", StringType(), True),
                    StructField("activity", StringType(), True),
                    StructField("activity_secondary", StringType(), True),
                    StructField("birth_day", LongType(), True),
                    StructField("birth_month", StringType(), True),
                    StructField("birthday", StringType(), True),
                    StructField("bottomsize", StringType(), True),
                    StructField("brasize", StringType(), True),
                    StructField("brasupport", StringType(), True),
                    StructField("color", StringType(), True),
                    StructField("email", StringType(), True),
                    StructField("error", StringType(), True),
                    StructField("gender", StringType(), True),
                    StructField("inseam", StringType(), True),
                    StructField("length", StringType(), True),
                    StructField("loyalty_membership_reward_tier_id", LongType(), True),
                    StructField("loyalty_membership_tier_points", LongType(), True),
                    StructField("membership_brand_id", LongType(), True),
                    StructField("membership_id", LongType(), True),
                    StructField("membership_level_group_id", LongType(), True),
                    StructField("membership_level_label", StringType(), True),
                    StructField("membership_reward_balance", LongType(), True),
                    StructField("membership_status", StringType(), True),
                    StructField("membership_statuscode", LongType(), True),
                    StructField("offer", StringType(), True),
                    StructField("referral_source", StringType(), True),
                    StructField("rise", StringType(), True),
                    StructField("topsize", StringType(), True),
                    StructField("total_num_credits", DoubleType(), True),
                    StructField("user_id", StringType(), True),
                    StructField("user_registered_at", StringType(), True),
                    StructField("zip_code", StringType(), True),
                ]
            ),
        ),
        StructField("type", StringType(), True),
        StructField("userId", StringType(), True),
        StructField("version", LongType(), True),
    ]
)
