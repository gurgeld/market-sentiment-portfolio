
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select id
from "warehouse"."main_silver"."stg_news"
where id is null



  
  
      
    ) dbt_internal_test