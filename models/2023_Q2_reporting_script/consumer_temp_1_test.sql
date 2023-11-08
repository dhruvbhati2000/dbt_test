{{ config(materialized='table') }}

with consumer_temp_1 as (
select 
            q1.uuid, 
            question_id as qid_1,
            answer_id as aid_1
        from tvc_surveys_prod.forsta_dev_prod.responses q1
        inner join (
    select uuid
    from tvc_surveys_prod.forsta_dev_prod.responses
    where question_id = 'status' and answer_id = '3' 
    ) q3 on q3.uuid = q1.uuid
        where contains(start_date,'2023') 
            and substr(start_date, 1, 2) in ('04', '05', '06')
            and survey_id = 'selfserve/2171/220259'
            and substr(question_id,1,2) in ('de', 'vc', 'ex','in', 'eg', 'fr', 'le', 'cl', 'su','ot')
union(
select 
--adding census region
            q1.uuid, 
            'demo11' as qid_1,
            substr(values_title,13, 100) as aid_1
        from tvc_surveys_prod.forsta_dev_prod.responses q1
        inner join tvc_surveys_prod.forsta_dev_prod.answers qa on q1.question_id = qa.question_id and q1.answer_id = to_char(qa.answer_id)
        inner join (
    select uuid
    from tvc_surveys_prod.forsta_dev_prod.responses
    where question_id = 'status' and answer_id = '3' 
    ) q3 on q3.uuid = q1.uuid
        where contains(start_date,'2023') 
            and substr(start_date, 1, 2) in ('04', '05', '06')
            and q1.survey_id = 'selfserve/2171/220259'
            and q1.question_id = 'vRegDIV'
)
union(
select 
-- adding education
            q1.uuid, 
            'demo10' as qid_1,
            values_title as aid_1
        from tvc_surveys_prod.forsta_dev_prod.responses q1
        inner join tvc_surveys_prod.forsta_dev_prod.answers qa on q1.question_id = qa.question_id and q1.answer_id = to_char(qa.answer_id)
        inner join (
    select uuid
    from tvc_surveys_prod.forsta_dev_prod.responses
    where question_id = 'status' and answer_id = '3' 
    ) q3 on q3.uuid = q1.uuid
        where contains(start_date,'2023') 
            and substr(start_date, 1, 2) in ('04', '05', '06')
            and q1.survey_id = 'selfserve/2171/220259'
            and q1.question_id = 'QEducation'
            )
),
consumer_temp_1_update as (
select 
    uuid
    -- ,qid_1
    -- ,aid_1
    ,case
        when qid_1 = 'demo6' then 2023::number - aid_1
        else aid_1
    end as aid_1
    ,case
        when qid_1 = 'otc5' then 'otc5w11'
        else qid_1
    end as qid_1
from consumer_temp_1 --where qid_1 = 'otc5'
),
consumer_temp_1_update_2 as (
select 
    uuid
    ,qid_1
    -- ,qid_2
    -- ,aid_1
    -- ,aid_2
    ,case
        when qid_1 = 'demo6' and aid_1 < 17 then 'Unknown'
        when qid_1 = 'demo6' and aid_1 < 25 then '18-24'
        when qid_1 = 'demo6' and aid_1 < 35 then '25-34'
        when qid_1 = 'demo6' and aid_1 < 45 then '35-44'
        when qid_1 = 'demo6' and aid_1 < 55 then '45-54'
        when qid_1 = 'demo6' and aid_1 < 65 then '55-64'
        when qid_1 = 'demo6' and aid_1 < 99 then '65+'
        else 'Unknown'
    end as aid_1
from consumer_temp_1_update
)
select * from consumer_temp_1_update_2
