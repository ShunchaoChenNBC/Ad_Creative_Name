CREATE OR REPLACE TABLE `nbcu-ds-sandbox-a-001.Shunchao_Sandbox.Ad_Creative_Part_One`
AS
WITH fwlogs_r AS (
  SELECT REGEXP_EXTRACT(AllRequestKV, '[?&]am_crmid=([^&]+)') AS fw_Id, 
        REGEXP_EXTRACT(AllRequestKV, '[?&]am_perid=([^&]+)') AS fw_PersonaId,
        UniqueIdentifier AS fw_UniqueIdentifier,
        TransactionId AS fw_TransactionId,
        VideoAssetId AS fw_VideoAssetId,
        --REGEXP_EXTRACT(AllRequestKV, '[?&]_fw_coppa=([^&]+)') AS fw_Coppa,
        --REGEXP_EXTRACT(AllRequestKV, '[?&]_fw_is_lat=([^&]+)') AS fw_is_lat,
        --REGEXP_EXTRACT(AllRequestKV, '[?&]_fw_atts=([^&]+)') AS fw_atts,
        --REGEXP_EXTRACT(AllRequestKV, '[?&]uoo=([^&]+)') AS fw_uoo,
        --REGEXP_EXTRACT(AllRequestKV, '[?&]am_cohort=([^&]+)') AS fw_Cohort,
        --REGEXP_EXTRACT(AllRequestKV, '[?&]am_extmp_ctrl=([^&]+)') AS fw_Control,
        REGEXP_EXTRACT(AllRequestKV, '[?&]am_abvrtd=([^&]+)') AS fw_Variant,
        --REGEXP_EXTRACT(AllRequestKV, '[?&]am_extmp=([^&]+)') AS fw_Template,
        --REGEXP_EXTRACT(AllRequestKV, '[?&]am_abtestid=([^&]+)') AS fw_Test,
        DATE(SDPBusinessDate) AS fw_Date
    FROM `nbcu-sdp-prod-003.sdp_persistent_views.FreewheelV4LogsView` 
    WHERE 1=1
        AND AllRequestKV IS NOT NULL
        AND UniqueIdentifier IS NOT NULL
        AND CustomVisitorId IS NOT NULL
        AND DATE(SDPBusinessDate) BETWEEN '2023-06-01' AND '2023-06-10'
    GROUP BY 1, 2, 3, 4, 5, 6, 7
),

fwlogs_i AS ( --FW impression logs of ad initiation 
    SELECT UniqueIdentifier AS fw_UniqueIdentifier, 
        TransactionId AS fw_TransactionId,
        AdUnitId AS fw_AdUnitId,
        CreativeId AS fw_CreativeId
    FROM `nbcu-sdp-prod-003.sdp_persistent_views.FreewheelV4LogsView` 
    WHERE EventType = 'I'
        AND DATE(SDPBusinessDate) BETWEEN '2023-06-01' AND '2023-06-10'
        AND AdUnitId IS NOT NULL
),

creative_name as (
SELECT creativeID,creativeName
FROM `nbcu-sdp-prod-003.sdp_persistent_views.FreewheelCreativeLookupView`
group by 1,2
), --- Creative_Name_Match

fwlogs_i_w_name AS (
select *
from fwlogs_i a
left join creative_name b on b.creativeID = a.fw_CreativeId
),

fwlogs_joined AS ( --join FW request with impression on UniqueIdentifier and TransactionId
    SELECT a.*, 
    b.fw_AdUnitId,
    b.creativeName
    FROM fwlogs_r a
    INNER JOIN fwlogs_i_w_name b ON a.fw_UniqueIdentifier = b.fw_UniqueIdentifier
        AND a.fw_TransactionId = b.fw_TransactionId
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
),
fw_profile_mapping AS ( --FW, mapping table
    SELECT ProfileId AS fw_ProfileId,
        ExternalProfilerID AS mfw_ExternalProfilerID
    FROM `nbcu-sdp-prod-003.sdp_persistent_views.CustomerKeysMapping`
    WHERE PartnerOrSystemId = 'freewheel'
),
fw_persona_mapping AS (
    SELECT householdId AS fw_HouseholdId,
        obfuscatedId AS mfw_ObfuscatedId,
        documentKey AS fw_DocumentKey
    FROM `nbcu-sdp-prod-003.sdp_persistent_views.PersonaKeyMappingView`
    WHERE obfuscatedIdName = 'freewheel'
),
fwm_joined as ( --join FWID with ExternalProfilerID from mapping table
    SELECT DISTINCT a.*, b.*, c.*
    FROM fwlogs_joined a
    INNER JOIN fw_profile_mapping b ON a.fw_Id = b.mfw_ExternalProfilerID
    INNER JOIN fw_persona_mapping c ON a.fw_PersonaId = c.mfw_ObfuscatedId
),
silver AS (
    SELECT adobe_tracking_id AS silver_Id,

        persona_id AS silver_PersonaId,
        adobe_timestamp AS silver_Date,
        video_id AS silver_VideoId, --post_evar122
        session_id AS silver_SessionId,
        -- post_evar95 not existing
        LOWER(program) AS silver_Program, --post_evar115
        LOWER(genre) AS silver_Genre,
        LOWER(franchise) AS silver_franchise,
        season AS silver_Season, --post_evar116
        LOWER(platform) AS silver_Platform, --post_evar106
        LOWER(stream_type) AS silver_StreamType, --post_evar109
        media_load AS silver_VideoInit, --20310
        num_views_started AS silver_VideoPlay, --20311
        num_views_reached_25 AS silver_Video25, --20314
        num_views_reached_50 AS silver_Video50, --20315
        num_views_reached_75 AS silver_Video75, --20316
        num_views_completed AS silver_VideoComplete, --20317
        ad_viewed AS silver_AdInit, --20323
        ad_served AS silver_AdComplete, --20324
        num_seconds_played_with_ads AS silver_VideoDuration, --20320
        num_seconds_played_no_ads AS silver_ContentTimePlay, --20319
        promo_break_position AS silver_AdPodPosition, --post_evar144
        promo_length AS silver_AdLength, --post_evar141
        promo_video_position AS silver_AdPodName, --post_evar143
        promo_campaign_name AS silver_AdName, --post_evar139
        asset_length AS silver_VideoLength, --post_evar124 -- diff from adobe
        consumption_type AS silver_ConsumptionType
    FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_VIDEO`
    WHERE adobe_date BETWEEN '2023-06-01' AND '2023-06-10'
        --AND LOWER(stream_type) = 'live'
        AND adobe_tracking_id IS NOT NULL
        AND persona_id IS NOT NULL
        AND adobe_tracking_id != 'N/A'
        AND persona_id != 'N/A'
),
silver_profile_mapping AS ( --Adobe, mapping table
    SELECT ProfileId AS adobe_ProfileId, 
        PartnerOrSystemId AS adobe_PartnerOrSystemId, 
        ExternalProfilerID AS madobe_ExternalProfilerID --, SDPBusinessDate AS SDPBusinessDate_madobe, SDPTimestamp AS SDPTimestamp_madobe
    FROM `nbcu-sdp-prod-003.sdp_persistent_views.CustomerKeysMapping`
    WHERE PartnerOrSystemId = 'trackingid'
    GROUP BY 1, 2, 3
), 
silver_persona_mapping AS (
    SELECT householdId AS adobe_HouseholdId, 
        obfuscatedId AS madobe_ObfuscatedId, 
        documentKey AS adobe_DocumentKey
    FROM `nbcu-sdp-prod-003.sdp_persistent_views.PersonaKeyMappingView`
    WHERE obfuscatedIdName = 'adobe_analytics'
),
silverm_joined AS ( --join Adobe_ID with ExternalProfilerID FROM mapping table
    SELECT a.*, b.*, c.*
    FROM silver a
    INNER JOIN silver_profile_mapping b ON a.silver_Id = b.madobe_ExternalProfilerID
    INNER JOIN silver_persona_mapping c ON a.silver_PersonaId = c.madobe_ObfuscatedId
)

SELECT DISTINCT a.fw_Date,
    b.silver_Id,
    b.silver_Date, 
    a.creativeName
FROM fwm_joined a
INNER JOIN silverm_joined b ON a.fw_ProfileId = b.adobe_ProfileId
    AND a.fw_DocumentKey = b.adobe_DocumentKey
    AND a.fw_Date = b.silver_Date
