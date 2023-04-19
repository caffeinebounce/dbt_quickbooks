/*
Table that creates a debit record to the specified expense account and credit record to accounts payable for each bill transaction.
*/

--To disable this model, set the using_bill variable within your dbt_project.yml file to False.
{{ config(enabled=var('using_bill', True)) }}

with bills as (

    select *
    from {{ ref('stg_quickbooks__bill') }}
),

bill_lines as (

    select *
    from {{ ref('stg_quickbooks__bill_line') }}
),

items as (
    
    select 
        item.*, 
        parent.expense_account_id as parent_expense_account_id,
        parent.income_account_id as parent_income_account_id
    from {{ ref('stg_quickbooks__item') }} item

    left join {{ ref('stg_quickbooks__item') }} parent
        on item.parent_item_id = parent.item_id
),

bill_join as (
    select
        bills.bill_id as transaction_id,
        bills.source_relation,
        bill_lines.index,
        bills.transaction_date,
        bill_lines.amount,
        coalesce(
            bill_lines.account_expense_account_id,
            bill_mapping_lines.account_expense_account_id,
            items.expense_account_id,
            items.parent_expense_account_id,
            items.expense_account_id,
            items.parent_income_account_id,
            items.income_account_id
        ) as payed_to_account_id,
        bills.payable_account_id,
        coalesce(bill_lines.account_expense_customer_id, bill_lines.item_expense_customer_id) as customer_id,
        coalesce(bill_lines.item_expense_class_id, bill_lines.account_expense_class_id) as class_id,
        bills.vendor_id
    from bills

    inner join bill_lines 
        on bills.bill_id = bill_lines.bill_id  
        and bills.source_relation = bill_lines.source_relation 

    left join items
        on bill_lines.item_expense_item_id = items.item_id
        and bill_lines.source_relation = items.source_relation

    left join (
        select bl2.bill_id, bl2.account_expense_account_id
        from bill_lines bl2
        where bl2.amount = 0
    ) as bill_mapping_lines
        on bill_lines.bill_id = bill_mapping_lines.bill_id
        and bill_lines.account_expense_account_id is null
),

final as (
    select 
        transaction_id,
        source_relation,
        index,
        transaction_date,
        customer_id,
        vendor_id,
        amount,
        payed_to_account_id as account_id,
        class_id,
        'debit' as transaction_type,
        'bill' as transaction_source
    from bill_join

    union all

    select
        transaction_id,
        source_relation,
        index,
        transaction_date,
        customer_id,
        vendor_id,
        amount,
        payable_account_id as account_id,
        class_id,
        'credit' as transaction_type,
        'bill' as transaction_source
    from bill_join
)

select *
from final