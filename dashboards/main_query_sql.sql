**-- The main query with correct spendings on acquisition from advertisment campaigns**

WITH 
num_of_users_by_group AS (
    SELECT count(DISTINCT user_id) AS user_tot_number
        , coalesce(inkoming_type, 'total') AS inkoming_type
    FROM user_id_source 
    GROUP BY ROLLUP(inkoming_type)
)
, num_actions_per_tarif_on_filtered AS (
    SELECT
        p.user_id,
        f.inkoming_type,
        p.datetime,
        p.action_type,
        s.subscription_type
    FROM
        payments_paid_actions_gptbot p
        INNER JOIN user_id_source  f
            ON p.user_id = f.user_id
        INNER JOIN payments_users_subscription s
            ON p.user_id = s.user_id
            AND p.datetime BETWEEN s.start_date AND s.end_date
    WHERE
        p.withdrawal_type = 'subscription'
)
, num_subscriptions_per_stuff AS (
    SELECT
        coalesce(f.inkoming_type, 'total') AS inkoming_type
        , s.subscription_type
        , COUNT(*) AS num_subscriptions_total
        , COUNT(*)::numeric/COUNT(DISTINCT user_id) AS num_subscriptions_per_user
    FROM
        user_id_source f
        INNER JOIN
        payments_users_subscription s
        USING(user_id)
    GROUP BY
        ROLLUP(f.inkoming_type),
        s.subscription_type
)
, avg_number_of_actions_per_subscription_per_stuff AS (
    SELECT 
         coalesce(inkoming_type, 'total') AS inkoming_type
        , subscription_type
        , SUM(CASE WHEN action_type = 'text' THEN 1 END)::float/(COUNT(DISTINCT user_id)) as num_actions_per_user_text_web
        , SUM(CASE WHEN action_type = 'picture_audio' THEN 1 END)::float/(COUNT(DISTINCT user_id)) as num_actions_per_user_pic_audio
        , SUM(CASE WHEN action_type = 'file_video' THEN 1 END)::float/(COUNT(DISTINCT user_id)) as num_actions_per_user_file_video
    FROM num_actions_per_tarif_on_filtered
    GROUP BY ROLLUP(inkoming_type), subscription_type
)
, avg_number_of_actions_per_subscription_per_stuff_corrected_multiple_subscriptions AS (
    -- RPR here!
    SELECT
        inkoming_type
        , subscription_type
        , num_actions_per_user_text_web::float / coalesce(num_subscriptions_per_user, 1) as num_actions_per_subscription_text_web 
        , num_actions_per_user_pic_audio::float / coalesce(num_subscriptions_per_user, 1) as num_actions_per_subscription_pic_audio
        , num_actions_per_user_file_video::float / coalesce(num_subscriptions_per_user, 1) as num_actions_per_subscription_file_video
    FROM avg_number_of_actions_per_subscription_per_stuff 
    LEFT JOIN
    num_subscriptions_per_stuff
    USING(inkoming_type, subscription_type)

)
, ratio_of_subs_usage_free AS (
    SELECT 
        inkoming_type
        , coalesce(num_actions_per_subscription_text_web/(50), 0.0) as ratio_text_web_free
        , coalesce(num_actions_per_subscription_pic_audio/(10), 0.0) as ratio_pic_audio_free
        , coalesce(num_actions_per_subscription_file_video/(3), 0.0) as ratio_doc_video_free
    FROM avg_number_of_actions_per_subscription_per_stuff_corrected_multiple_subscriptions_free
    WHERE subscription_type = 'free'
)
, avg_actions_price AS (
    SELECT 
    coalesce(inkoming_type, 'total') AS inkoming_type
    , AVG(CASE WHEN action_type = 'text' THEN cost END) AS avg_cost_text_web
    , AVG(CASE WHEN action_type = 'picture_audio' THEN cost END) AS avg_cost_pic_audio
    , AVG(CASE WHEN action_type = 'file_video' THEN cost END) AS avg_cost_file_video
    , COUNT(CASE WHEN action_type = 'text' THEN 1 END) AS num_actions_text_web
    , COUNT(CASE WHEN action_type = 'picture_audio' THEN 1 END) AS num_actions_pic_audio
    , COUNT(CASE WHEN action_type = 'file_video' THEN 1 END) AS num_actions_file_video
    , SUM(CASE WHEN action_type = 'text' THEN cost END) AS spent_total_actions_text_web
    , SUM(CASE WHEN action_type = 'picture_audio' THEN cost END) AS spent_total_actions_pic_audio
    , SUM(CASE WHEN action_type = 'file_video' THEN cost END) AS spent_total_actions_file_video
    , SUM(cost) AS total_spent_api -- it does not include 40 usd for mijourney and deeply
    , COUNT(*) AS paid_operations_count
    FROM payments_paid_actions_gptbot
    INNER JOIN
    user_id_source
    USING (user_id)
    GROUP BY ROLLUP(inkoming_type)
)
, income_usd AS ( -- taxes are not included in calculations
    SELECT 
        source_name_user_id
        , details
        , purpose
        , purpose || ' ' || details as combined_paid_for_name
        , CASE
            WHEN recieved_currency = 'RUB' then (recieved::float / 100) --convert RUB to USD 
            WHEN recieved_currency = 'XTR' THEN (recieved::float / 80) -- Convert XTR to USD
        	ELSE 0 -- Handle unexpected currencies
        END AS income_usd
    FROM payments_income_bugalteria
)
, income_agg AS (
    SELECT 
    coalesce(inkoming_type, 'total') AS inkoming_type
    , SUM(income_usd) AS total_income
    , COUNT(*) AS num_paid_times
    , COUNT(CASE WHEN combined_paid_for_name = 'package Photo&Audio' THEN 1 END) as num_bought_pkg_photo_audio
    , COUNT(CASE WHEN combined_paid_for_name = 'package File&Video' THEN 1 END) as num_bought_pkg_file_video
    , COUNT(CASE WHEN combined_paid_for_name = 'package Text' THEN 1 END) as num_bought_pkg_text_web
    , COUNT(CASE WHEN details = 'basic' THEN 1 END) as num_bought_tarif_basic -- it is subscription basic
    , COUNT(CASE WHEN details = 'text&web' THEN 1 END) as num_bought_tarif_text
    , COUNT(CASE WHEN details = 'small' THEN 1 END) as num_bought_tarif_small -- subscr small
    , COUNT(CASE WHEN details = 'medium' THEN 1 END) as num_bought_tarif_medium -- subscr medium
    , COUNT(DISTINCT user_id) AS num_user_who_pays
    FROM income_usd pib
    INNER JOIN
    user_id_source fbr
    ON fbr.user_id = pib.source_name_user_id
    GROUP BY ROLLUP(inkoming_type)
)
, all_inkoming_types AS (
    SELECT DISTINCT inkoming_type
    FROM num_of_users_by_group
)
, acquisition_spent AS ( 
	SELECT inkoming_type, coalesce(SUM(total_cost), 0.0) AS total_spent_acquisition
	FROM all_inkoming_types
	LEFT JOIN
	gptbot_ads_compains gac ON all_inkoming_types.inkoming_type = gac.campain_name
	WHERE inkoming_type != 'total'
	GROUP BY inkoming_type
)
, acquisition_price AS (
	SELECT coalesce(inkoming_type, 'total') AS inkoming_type
	, CASE
		WHEN inkoming_type = 'users_referal' THEN 0.0992 -- calculated in average action price for users case by referral
		ELSE SUM(total_spent_acquisition) 
	END  AS total_spent_acquisition_corrected
	FROM acquisition_spent
	GROUP BY ROLLUP(inkoming_type)
)

, all_basic_joined AS (
    SELECT *
    FROM num_of_users_by_group
    LEFT JOIN income_agg
    USING(inkoming_type)
    LEFT JOIN avg_actions_price
    USING(inkoming_type)
    LEFT JOIN acquisition_price
    USING(inkoming_type)
    LEFT JOIN ratio_of_subs_usage_free
    USING(inkoming_type)
    LEFT JOIN profit_margin
    USING(inkoming_type)
    LEFT JOIN cost_of_free_period
    USING(inkoming_type)
    LEFT JOIN free_package_use_rate
    USING(inkoming_type)
    LEFT JOIN avg_price_of_packages
    USING(inkoming_type)
)
, ready_for_calcualtions AS (
    SELECT
        inkoming_type
        , user_tot_number
        , coalesce(total_income, 0.0) as total_income_usd
        , coalesce(num_paid_times, 0.0) as payment_operations
        , coalesce(num_user_who_pays, 0.0) as uniq_users_paid
        , coalesce(num_bought_pkg_photo_audio, 0.0) as num_bought_pkg_photo_audio
        , coalesce(num_bought_pkg_file_video, 0.0) as num_bought_pkg_file_video
        , coalesce(num_bought_pkg_text_web, 0.0) as num_bought_pkg_text_web
        , coalesce(num_bought_tarif_basic, 0.0) as num_bought_tarif_basic
        , coalesce(num_bought_tarif_text, 0.0) as num_bought_tarif_text
        , coalesce(num_bought_tarif_small, 0.0) as num_bought_tarif_small
        , coalesce(num_bought_tarif_medium, 0.0) as num_bought_tarif_medium
        , coalesce(avg_cost_text_web, 0.0) as avg_cost_text_web
        , coalesce(avg_cost_pic_audio, 0.0) as avg_cost_pic_audio
        , coalesce(avg_cost_file_video, 0.0) as avg_cost_file_video
        , coalesce(num_actions_text_web, 0.0) as num_actions_text_web
        , coalesce(num_actions_pic_audio, 0.0) as num_actions_pic_audio
        , coalesce(num_actions_file_video, 0.0) as num_actions_file_video
        , coalesce(spent_total_actions_text_web, 0.0) as spent_total_actions_text_web
        , coalesce(spent_total_actions_pic_audio, 0.0) as spent_total_actions_pic_audio
        , coalesce(spent_total_actions_file_video, 0.0) as spent_total_actions_file_video
        , coalesce(total_spent_api, 0.0) as total_spent_api
        , coalesce(paid_operations_count ,0.0) as paid_operations_count
        , coalesce(ratio_text_web_free, 0.0) as ratio_text_web_free
        , coalesce(ratio_pic_audio_free, 0.0) as ratio_pic_audio_free
        , coalesce(ratio_doc_video_free, 0.0) as ratio_doc_video_free
        , total_spent_acquisition_corrected
        , coalesce(profit_margin, 0.0) as profit_margin
        , coalesce(cost_of_free_packages_usd, 0.0) AS cost_of_free_packages_usd--coalesce(cost_of_packages_per_ab_test_user_usd, 0.0) as cost_of_packages_per_ab_test_user_usd 
        , coalesce(cost_of_free_gifts_usd, 0.0) as cost_of_free_gifts_usd
        , coalesce(cost_on_api_free_subscription_usd, 0.0) as cost_on_api_free_subscription_usd
        , coalesce(avg_pct_spent_from_package, 1) as avg_pct_spent_from_package
        , coalesce(avg_pct_spent_from_gift, 1) as avg_pct_spent_from_gift
        , coalesce (avg_package_price_usd, 2) as avg_package_price_usd -- Assuming that avg price is 2$ if not available in avg_price_of_packages View
    FROM all_basic_joined
    ORDER BY user_tot_number DESC
)
, created_referrals AS
-- how many people made referrals
(SELECT 
coalesce(COUNT(prp.referal_id), 0) as number_created_referral_links
, coalesce(COUNT(distinct prp.user_id), 0) as number_users_created_referral_links
, coalesce(uis.inkoming_type, 'other_user_source') as incoming_type
FROM payments_referral_program prp
LEFT JOIN user_id_source uis USING(user_id)
GROUP BY uis.inkoming_type
)
-- how many people used referral
, users_referral AS 
(SELECT prp.user_id
, prp.referal_id
, prp.datetime as created_referral_datetime
, prp.user
, pra.datetime as used_referral_datetime
, pra.status
FROM payments_referral_program prp
LEFT JOIN payments_referral_actions pra USING (referal_id)
WHERE pra.status = 'success'
)

, used_referrals as
(SELECT coalesce(count(distinct ur.user_id),0) AS users_used_referral,
coalesce(uis.inkoming_type, 'other_user_source') as incoming_type
FROM users_referral ur
LEFT JOIN user_id_source uis USING (user_id)
GROUP BY uis.inkoming_type
)

, referrals_final AS (
SELECT cr.incoming_type
, cr.number_users_created_referral_links
, cr.number_created_referral_links
, coalesce(ur.users_used_referral, 0) as users_used_referral
FROM created_referrals cr
LEFT JOIN used_referrals ur USING (incoming_type)
)

, ready_fin as (
    SELECT 
        inkoming_type AS incoming_type
        , user_tot_number
        , total_income_usd
        , payment_operations
        , uniq_users_paid
        , num_bought_pkg_photo_audio
        , num_bought_pkg_file_video
        , num_bought_pkg_text_web
        , num_bought_tarif_basic
        , num_bought_tarif_text
        , num_bought_tarif_small
        , num_bought_tarif_medium
        , avg_cost_text_web
        , avg_cost_pic_audio
        , avg_cost_file_video
        , ratio_text_web_free
        , ratio_pic_audio_free
        , ratio_doc_video_free
        , num_actions_text_web
        , num_actions_pic_audio
        , num_actions_file_video
        , spent_total_actions_text_web
        , spent_total_actions_pic_audio
        , spent_total_actions_file_video
        , total_spent_api
        , paid_operations_count
        , total_spent_acquisition_corrected
        , COALESCE(cac,0) as cac
        , number_users_created_referral_links
        , number_created_referral_links
        , users_used_referral
        , profit_margin
        , avg_package_price_usd
        , (cost_of_free_packages_usd * avg_pct_spent_from_package)::float/user_tot_number AS cost_of_free_packages_per_user_usd--cost_of_packages_per_ab_test_user_usd 
        , (cost_of_free_gifts_usd * avg_pct_spent_from_gift)::float/user_tot_number AS cost_of_free_gifts_per_user
        , cost_on_api_free_subscription_usd::float/user_tot_number AS cost_on_api_free_subscription_per_user--cost_on_API_free_subscription__per_user_usd
    FROM ready_for_calcualtions
    LEFT JOIN cac_materialized_view ON ready_for_calcualtions.inkoming_type = cac_materialized_view.incoming_type
    LEFT JOIN referrals_final ON ready_for_calcualtions.inkoming_type = referrals_final.incoming_type
    WHERE user_tot_number > 0
)

, first_complex_calculations AS (
    SELECT 
    -- *
         incoming_type
        , user_tot_number
        , total_income_usd
        , payment_operations
        , uniq_users_paid
        , total_spent_api
        , paid_operations_count
        , total_spent_acquisition_corrected
        , profit_margin
        , cac
        , cost_of_free_packages_per_user_usd --cost_of_packages_per_ab_test_user_usd AS cost_of_free_packages_per_ab_test_user_usd 
        , cost_of_free_gifts_per_user
        , cost_on_api_free_subscription_per_user
        , number_users_created_referral_links
        , number_created_referral_links
        , users_used_referral
        , avg_package_price_usd
        , coalesce(total_spent_acquisition_corrected / user_tot_number, 0) as CPU
        , CASE
            WHEN uniq_users_paid > 0 then total_spent_acquisition_corrected / uniq_users_paid 
            ELSE NULL
        END as CPPU
        , CASE
            WHEN user_tot_number > 0 then (uniq_users_paid / user_tot_number) 
            ELSE NULL
        END as conversion_to_payments
    FROM ready_fin
)
, final_calc AS ( -- unless calculated in free_package_use_rate view, ratio of free gifts and packages is 100%
SELECT * 
, conversion_to_payments*profit_margin*avg_package_price_usd*2.5 - cac - cost_of_free_packages_per_user_usd - cost_of_free_gifts_per_user - cost_on_api_free_subscription_per_user AS CLTV 

SELECT * from final_calc



