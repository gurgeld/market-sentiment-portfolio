
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select ticker
from "warehouse"."main_silver"."stg_ticker_sentiment"
where ticker is null



  
  
      
    ) dbt_internal_test