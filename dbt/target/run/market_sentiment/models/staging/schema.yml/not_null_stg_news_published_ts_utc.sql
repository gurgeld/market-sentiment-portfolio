
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select published_ts_utc
from "warehouse"."main_silver"."stg_news"
where published_ts_utc is null



  
  
      
    ) dbt_internal_test