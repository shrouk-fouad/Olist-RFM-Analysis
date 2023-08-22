/*create a view of 3 clasees of customers based on paid ampounts*/
alter view customer_classes
with encryption
as
(
select  id, Gender, Age,purchase, case
										when G = 3 then 'Class A'
										when G = 2 then 'Class B'
										else 'Class C' end as Class
from (
select ntile(3)over(order by sum(payment_value)) as G, id, Gender, Age, sum(payment_value) as purchase
from customers c join orders o
	on c.id = o.customer_id
join Payments p 
	on o.order_id = p.order_id
group by  id, Gender, Age
)as new_table
)

select  * from customer_classes
order by purchase desc

sp_helptext customer_classes





/*Create a view of returned customers and the intervals in months between 2 purchases*/
create view purchasess_interval
as
(
select unique_id,purchase_date, nxt_purchase, case 
												when DATEDIFF(day, purchase_date, nxt_purchase)/30 = 0 then 1
												else DATEDIFF(day, purchase_date, nxt_purchase)/30 end as interval_months
from
(
select unique_id, purchase_date, LEAD(purchase_date)over(partition by unique_id order by purchase_date) as nxt_purchase
from customers c join orders o
on c.id = o.customer_id) as new
where nxt_purchase is not null)

select * from purchasess_interval


/*create view of custoemrs with cohort_month*/
alter view cohort_month
as
select unique_id, cast(DATEADD(day,1 - DAY(purchase_date), purchase_date) as date)as cohort_month
from
(select unique_id,purchase_date, min(purchase_date) as cohort_day
from customers c join orders o
on c.id = o.customer_id
group by unique_id,purchase_date) as new


/*Create a view of cohorts counts of each month*/
create view cohort_counts
as
(

SELECT
        cohort_month,COUNT(unique_id) AS customer_count
    FROM cohort_month
    GROUP BY cohort_month
)
go
select * from cohort_counts
order by cohort_month


/*Create a view of RFM Segments*/

alter view RFM_Segmentation
with Encryption
as
(
select *, NTILE(4) over(order by Recency desc) as R,
		  NTILE(4) over(order by Frequency) as F,
		  NTILE(4) over(order by Monetary) as M, 
		  concat(NTILE(4) over(order by Recency desc) ,NTILE(4) over(order by Frequency) , NTILE(4) over(order by Monetary) ) as RFM_segmentation,
          NTILE(4) over(order by Recency desc) +NTILE(4) over(order by Frequency) + NTILE(4) over(order by Monetary) as RFM_Score
from
(
	select unique_id, 
			DATEDIFF(day,max(purchase_date),(select DATEADD(DAY,1,max(purchase_date))   
											from orders)) as Recency, /* Snapshot is the date point to analyze data before
																						select DATEADD(DAY,1,max(purchase_date))
																						from orders */
			count(o.order_id) as Frequency,
			sum(price*items_no) as Monetary
	from customers c join orders o
		on c.id = o.customer_id
		join Order_Items i
		on i.order_id = o.order_id
	where purchase_date >= '2017-10-17' /*to make sure snapshot is only 12 months*/
	group by unique_id) as new
)

select * from RFM_Segmentation





/*Create non clustered index on state column*/
create nonclustered index i1
on locations(state)


/*Display Revenue for each order using cursor*/
declare c1 cursor
for select unique_id, sum(price*items_no) as Revenue
	from customers c join orders o
		on c.id = o.customer_id
		join Order_Items i
		on i.order_id = o.order_id
	group by unique_id

for read only

declare @id varchar(50), @revenue float
open c1
fetch c1 into @id, @revenue
while @@FETCH_STATUS = 0
begin
	select @id as customer_id, @revenue as Revenue
	fetch c1 into @id, @revenue
end
close c1
deallocate c1


/*increase prices by 20% using cursor*/
declare c1 cursor
for select price
	from Order_Items 

for update

declare  @price float

open c1
fetch c1 into @price

while @@FETCH_STATUS = 0
	begin
	update Order_Items
	set price = @price*1.20
	where current of c1 

	fetch c1 into @price
	end

close c1
deallocate c1

select price from Order_Items


/*create sp to add new customers into Customers table when they are not already exists and roll back if exists*/
create proc AddCustomer @id varchar(50), @uniqueid varchar(50), @gender varchar(10), @age int, @location int
with encryption
as
	begin try
		insert into Customers
		values ( @id, @uniqueid, @gender, @age, @location)
	end try
	begin catch 
		select 'Customer Already Exists'
	end catch

AddCustomer '00000161a058600d5901f007fab4c27140','b000545015e09bb4b6e47c52844fab5fb6638','Male',40,13802


/*Create dynamic view using SP to get all customers of given state*/
create proc GetCustomerInfo @state varchar(20)
with encryption
as
	select c.*
	from Customers c join Locations l
	on c.location_id = l.location_id
	where state = @state

GetCustomerInfo 'Rio de Janeiro'


/*Create a dynamic query to display a column from a table*/
create proc dynamic_query @col varchar(20), @tab varchar(10)
as 
	execute ('select '+ @col + ' from ' + @tab)

dynamic_query '*', 'orders'

/*Create sp that dispalays sellers who sells items whith prices higher than average items price*/

create proc Expensive_Sellers
as
	declare @avg_prices float
	select @avg_prices = avg(price)
	from
		(
			select distinct p.product_id, price
			from Products p join Order_Items i
			on p.product_id = i.product_id
		) as new_table

	select distinct s.*
	from sellers s join Order_Items i
	on i.seller_id = s.seller_id
	where price > @avg_prices

declare @t table (seller_id varchar(max), location_id int)
insert into @t
execute Expensive_Sellers
select * from @t



/*Create SP to get subcategories of prices between 2 given prices* >> using Insert based on EXECUTE*/
alter proc SubcategoryList @price1 float, @price2 float
with encryption
as
	select distinct p.SubCategory
	from Products p join Order_Items i
	on i.product_id = p.product_id
	where price between @price1 and @price2

declare @t table(subcategory varchar(max))
insert into @t
Execute SubcategoryList 10.0,20.0
select * from @t



/*Create SP to return order ID and status into variables that can be used into another function or SP given customer ID*/
create proc OrderInfo @customerid varchar(max), @order_id varchar(max) output, @status varchar(20) output
with encryption 
as
	select @order_id = order_id, @status = status
	from Orders o join Customers c
	on o.customer_id = c.id
	where unique_id = @customerid

declare @x varchar(max), @y varchar(20)
Execute OrderInfo 'b0015e09bb4b6e47c52844fab5fb6638', @x output, @y output
select @x, @y 




/*Create Trigger to Welcome new users*/
create trigger t1
on customers
for insert
as 
	Select 'Welcome To Olist'

AddCustomer '000000161a058600d5901f007fab4c27140','b0000545015e09bb4b6e47c52844fab5fb6638','Male',40,13802


/*Create Trigger to roll back alters on olist database when it's friday*/
create trigger t2
on database
for alter_table
as
	if format(getdate(),'dddd') = 'friday'
		begin
			rollback
			select 'Cant make changes on Database during weekends'
		end


/*Create Audit table for any updates, inserts, deletes on orders_items table*/
create table History
(login_user varchar(20),
datee date,
actionn varchar(20))



create trigger t3 
on order_items
for insert, update, delete
as 
	declare @action varchar(20)
	if exists (Select order_details_id from inserted) and  exists (Select order_details_id from deleted)
		set @action = 'Update'
	else if exists (Select order_details_id from inserted) and  not  exists (Select order_details_id from deleted)
		set @action = 'Insert'
	else if not exists (Select order_details_id from inserted) and  exists (Select order_details_id from deleted)
		set @action = 'Delete'
	insert into History
	values(SUSER_ID(), GETDATE(), @action)

delete from Order_Items
where order_details_id = 100

select * from History



/*********************************************************	PIVOT ************************************************/
/*Display counts of item orders for each product category pivoting category in columns*/
select * from
(select Category, Row_number()over(partition by Category order by i.product_id) as RN
from Products p join Order_Items i
on p.product_id = i.product_id) as source_table
pivot
(count(RN) for category in ([Furniture],[Office Supplies],[Technology])) as pv



/*display subcategories for each product category in a list where categories are pivoted as columns*/
select [Furniture],[Office Supplies],[Technology] 
from 
(select Category, SubCategory, row_number()over(partition by Category order by SubCategory) as RN
from (select distinct Category, SubCategory
from Products) as new_table
) as source_table
pivot
(max(SubCategory) for category in ([Furniture],[Office Supplies],[Technology])) as pv




/******************************************* RollUp *******************************************************/
/*display count of items for each subcategory within a category
and add a row of total items in all subcategory within the catory after each category*/
select Category, SubCategory, count(*) as items
from Products p join Order_Items i
on p.product_id = i.product_id
group by rollup(Category, SubCategory)



/**************************************** FUNCTOINS *********************************************/
/*create a function that displays the status order given an order id */
create function GetStatus(@order_id varchar(max))
returns varchar(20)
	begin

	declare @status varchar(20)
	select @status = status
	from orders
	where order_id = @order_id

	return @status
	end

select dbo.GetStatus('000229ec398224ef6ca0657da4fc703e')






/*Create a functoin that takes customer id as a parameter and returns 'High Priorty Customer if his purchases
is more than average purchases per customer*/
create function CustomerCategory(@id varchar(max))
returns varchar(20)
begin
	declare @avg_purchases float, @category varchar(20), @total_purchases float
	select @avg_purchases = avg(total_purchases)
	from
		(
			select unique_id, sum(price * items_no) as total_purchases
			from Customers c join orders o
			on c.id = o.customer_id
			join Order_Items i 
			on i.order_id = o.order_id
			group by unique_id) as new_table

	select @total_purchases = sum(price * items_no)
	from Customers c join orders o
	on c.id = o.customer_id
	join Order_Items i 
	on i.order_id = o.order_id
	where c.unique_id = @id
	group by unique_id
			
	if @total_purchases > @avg_purchases
		set @category = 'High Priority Customer'
	else set @category = 'Low Priority Customer'
return @category
end

select dbo.CustomerCategory('b0000545015e09bb4b6e47c52844fab5fb6638')


/*create a function that returns all payment transaction for a customer given payment customer id*/
create function CustomerPayments(@id varchar(max))
returns table
return
	(select p.*
	from Customers c join Orders o
	on c.id = o.customer_id
	join Payments p
	on p.order_id = o.order_id
	where unique_id = @id)


select * from CustomerPayments ('ca0cd77819a2427d6ca0e0d0e92de58b')



/*create function that returns customer id and 
'Satisfied' if his average review scores higher than 4
'Neutral' if higher than 3
'Unsatisfied' if below 3*/

create function CustumorSatisfaction (@satisfaction_level varchar(10))
returns @t table (customer_id varchar(max), Rate int, Satisfaction_level varchar(10))
begin
	if @satisfaction_level = 'Satisfied'
		begin
			insert into @t
			select * 
			from
			(
				select unique_id, AVG(review_score) as rating, case
												when AVG(review_score) > 4 then 'Satisfied'
												when AVG(review_score) > 3 and AVG(review_score) <= 4 then 'Neutral'
												else 'Unsatisfied' end as Satisfacion_level
				from Customers c join orders o
				on o.customer_id = c.id
				join Reviews r
				on r.order_id = o.order_id
				group by unique_id) as new_table
				where Satisfacion_level = 'Satisfied'
		end
	else if @satisfaction_level = 'Neutral'
		begin
			insert into @t
			select * 
			from
			(
				select unique_id, AVG(review_score) as rating, case
												when AVG(review_score) > 4 then 'Satisfied'
												when AVG(review_score) > 3 and AVG(review_score) <= 4 then 'Neutral'
												else 'Unsatisfied' end as Satisfacion_level
				from Customers c join orders o
				on o.customer_id = c.id
				join Reviews r
				on r.order_id = o.order_id
				group by unique_id) as new_table
				where Satisfacion_level = 'Neutral'
		end

		else if @satisfaction_level = 'Unsatisfied'
		begin
			insert into @t
			select * 
			from
			(
				select unique_id, AVG(review_score) as rating, case
												when AVG(review_score) > 4 then 'Satisfied'
												when AVG(review_score) > 3 and AVG(review_score) <= 4 then 'Neutral'
												else 'Unsatisfied' end as Satisfacion_level
				from Customers c join orders o
				on o.customer_id = c.id
				join Reviews r
				on r.order_id = o.order_id
				group by unique_id) as new_table
				where Satisfacion_level = 'Unsatisfied'
		end	
return 
end

select * from CustumorSatisfaction('Satisfied')