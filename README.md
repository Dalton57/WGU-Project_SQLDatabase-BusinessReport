# WGU-Project_SQLDatabase-BusinessReport


## **Data Analysis Business Report**


- ### A 

    - This business report will analyze what films are most profitable in our DVD rental shops. 
The report will compare the average price that customers have spent on a film to 
the total sales each film has made by categorizing the average prices into three tiers. 
The three tiers are separated into low, mid, and high. 


- ### B
    - The report will have two separate tables to organize the data - detailed and summary tables. 
The detailed table, which includes the **‘film_id’**, **'film_title’**, **‘payment_amount’**, and **‘price_category’** fields, 
are used to gather the raw data. In the summary table, the **‘film_title’**, **'total_sales’**, **‘units_sold’**,
      **‘avg_sale_per_unit’**, and **‘price_tier’** fields are used. 

- ### C
    - The data fields used for this report are **‘TEXT’** for the film titles; **‘NUMERIC (5, 2)** for the dollar amounts;
      **‘INT’** for the number of sales made per film; and **‘CHAR(4)’** for the three pricing tiers to help with readability.

- ### D
    - The tables that will provide the data necessary for this report are the **‘film’** and **‘payment’** tables.

- ### E
    - The **'payment_amount'** field in the detailed table will have to be transformed into a **price tier** to show 
what the average prices are ranging from **low**, **mid**, and **high**.

- ### F
    - With the two tables, **detailed** and **summary**, we can gather two perspectives in the data. 
  With the detailed table we can see each payment amount for a film individually and with the 
  summary table we can see each individual film with the **total sales**, **units sold**,
      **average price sold**, and what **pricing tier** the film normally falls in.

- ### G
  - This report should be refreshed quarterly for stakeholders to stay relevant and keep track of any trends. 
  This would allow stakeholders and store leadership to make adjustments to pricing as necessary.

- ### H
    - This code is for the transformation of the payment_amount field to price tiers previously mentioned in **E**:

    
    CREATE OR REPLACE FUNCTION price_tier(avg_sale_per_unit NUMERIC)
    RETURNS TEXT AS $$
    DECLARE
        tier TEXT;
    BEGIN
        IF avg_sale_per_unit <= 3.99 THEN
            tier := 'Low';
        ELSIF avg_sale_per_unit >= 7.99 THEN
            tier := 'High';
        ELSE
            tier := 'Mid';
        END IF;
        RETURN tier; 
    END;
    $$ LANGUAGE plpgsql;

- ### I
    - This is the code used to create the detailed and summary tables:


    CREATE TABLE detailed_table (
        film_id INT,
        film_title TEXT,
        payment_amount NUMERIC (5, 2)
        );

    CREATE TABLE summary_table (
        film_title TEXT,
        total_sales NUMERIC (5, 2),
        units_sold INT,
        avg_sale_per_unit NUMERIC (5, 2),
        price_tier CHAR(4)
        );

- ### J
    - This is the code used to extract the raw data needed for the detailed table:


    INSERT INTO detailed_table (film_id, film_title, payment_amount)
    SELECT film.film_id, film.title, payment.amount
    FROM film
    JOIN inventory ON film.film_id = inventory.film_id
    JOIN rental ON inventory.inventory_id = rental.inventory_id
    JOIN payment ON rental.rental_id = payment.rental_id;

- ### K
    - This is the code for the function and trigger on the detailed table that will continually update 
  the summary table as data is added to the detailed table:


    CREATE OR REPLACE FUNCTION update_summary_from_detailed()
    RETURNS TRIGGER AS $$
    BEGIN
        IF EXISTS (SELECT 1 FROM summary_table WHERE film_title = NEW.film_title) 
    THEN
            UPDATE summary_table
            SET
                total_sales = total_sales + NEW.payment_amount,
                units_sold = units_sold + 1,
                avg_sale_per_unit = ROUND((total_sales + NEW.payment_amount) / (units_sold + 1), 2),
                price_tier = price_tier(ROUND((total_sales + NEW.payment_amount) / (units_sold + 1), 2))
            WHERE film_title = NEW.film_title;
        ELSE
            INSERT INTO summary_table (
                film_title,
                total_sales,
                units_sold,
                avg_sale_per_unit,
                price_tier
            ) VALUES (
                NEW.film_title,
                NEW.payment_amount,
                1,
                ROUND(NEW.payment_amount, 2),
                price_tier(ROUND(NEW.payment_amount, 2))
            );
        END IF;
    
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
    
    
    
    
    CREATE TRIGGER trigger_update_summary_from_detailed
    AFTER INSERT ON detailed_table
    FOR EACH ROW
    EXECUTE FUNCTION update_summary_from_detailed();

- ### L
    - This is the code that will refresh both detailed and summary tables and 
perform a raw data extraction to the detailed table.

~~~
    CREATE OR REPLACE PROCEDURE refresh_summary_and_detailed_tables()
    LANGUAGE plpgsql
    AS $$
    BEGIN
        TRUNCATE TABLE summary_table;
        TRUNCATE TABLE detailed_table;
    
        INSERT INTO detailed_table (film_id, film_title, payment_amount)
        SELECT film.film_id, film.title, payment.amount
        FROM film
        JOIN inventory ON film.film_id = inventory.film_id
        JOIN rental ON inventory.inventory_id = rental.inventory_id
        JOIN payment ON rental.rental_id = payment.rental_id;

    END;
    $$;
~~~

- ### M
    - The job scheduling will be using a .bat file to store the command line for PostgreSQL 
  that will execute the procedure and Task Scheduler to periodically execute the 
  command daily, weekly, monthly, or as needed. This will only be able to work on Windows machines 
  to utilize the job scheduling. This is the .bat file command that will be used:


    @echo off
    SET PGPASSWORD=(yourpassword)
    “C: \Program FIles\PostgreSQL\13\bin\psql.exe” -U postgres -d dvdrental -h localhost -c "CALL refresh_summary_and_detailed_tables();"
