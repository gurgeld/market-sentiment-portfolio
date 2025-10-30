
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select overall_sentiment_score
from "warehouse"."main_silver"."stg_news"
where overall_sentiment_score is null



  
  
      
    ) dbt_internal_test