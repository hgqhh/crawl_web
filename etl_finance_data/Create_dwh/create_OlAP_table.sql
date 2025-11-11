-- Create Table price_predict
CREATE TABLE `price_predict` (
	time DATETIME,
    ticker nvarchar(10),
    price_predict decimal(5,2)
);

-- CREATE TABLE Stage_FinancialStatement
CREATE TABLE Stage_FinancialStatement as
select 
	b.*,
	revenue, yearRevenueGrowth, quarterRevenueGrowth, costOfGoodSold, grossProfit, operationExpense, operationProfit, yearOperationProfitGrowth, quarterOperationProfitGrowth, interestExpense, preTaxProfit, postTaxProfit, shareHolderIncome, yearShareHolderIncomeGrowth, quarterShareHolderIncomeGrowth, investProfit, serviceProfit, otherProfit, provisionExpense, operationIncome, ebitda,
    investCost, fromInvest, fromFinancial, fromSale, freeCashFlow
from balance_sheet b join income i on b.ticker = i.ticker and b.year = i.year and b.quarter = i.quarter
join cash_flow cf on b.ticker = cf.ticker and b.year = cf.year and b.quarter = cf.quarter;

-- UNpivot table Stage_FinancialStatement to Fact_FinancialStatement
Create TABLE Fact_FinancialStatement as
select ticker, year, quarter, 'shortAsset' as financial_index, shortAsset as value
from Stage_FinancialStatement
union all
select ticker, year, quarter, 'shortInvest' as financial_index, shortInvest as value
from Stage_FinancialStatement
union all
select ticker, year, quarter, 'shortReceivable' as financial_index, shortReceivable as value
from Stage_FinancialStatement
union all
select ticker, year, quarter, 'inventory' as financial_index, inventory as value
from Stage_FinancialStatement
union all
select ticker, year, quarter, 'longAsset' as financial_index, longAsset as value
from Stage_FinancialStatement
union all
select ticker, year, quarter, 'fixedAsset' as financial_index, fixedAsset as value
from Stage_FinancialStatement
union all
select ticker, year, quarter, 'asset' as financial_index, asset as value
from Stage_FinancialStatement
union all
select ticker, year, quarter, 'debt' as financial_index, debt as value
from Stage_FinancialStatement
union all
select ticker, year, quarter, 'shortDebt' as financial_index, shortDebt as value
from Stage_FinancialStatement
union all
select ticker, year, quarter, 'longDebt' as financial_index, longDebt as value
from Stage_FinancialStatement
union all
select ticker, year, quarter, 'equity' as financial_index, equity as value
from Stage_FinancialStatement
union all
select ticker, year, quarter, 'capital' as financial_index, capital as value
from Stage_FinancialStatement
union all
select ticker, year, quarter, 'minorShareHolderProfit' as financial_index, minorShareHolderProfit as value
from Stage_FinancialStatement
union all
select ticker, year, quarter, 'payable' as financial_index, payable as value
from Stage_FinancialStatement
union all
select ticker, year, quarter, 'revenue' as financial_index, revenue as value
from Stage_FinancialStatement
union all
select ticker, year, quarter, 'yearRevenueGrowth' as financial_index, yearRevenueGrowth as value
from Stage_FinancialStatement
union all
select ticker, year, quarter, 'quarterRevenueGrowth' as financial_index, quarterRevenueGrowth as value
from Stage_FinancialStatement
union all
select ticker, year, quarter, 'costOfGoodSold' as financial_index, costOfGoodSold as value
from Stage_FinancialStatement
union all
select ticker, year, quarter, 'grossProfit' as financial_index, grossProfit as value
from Stage_FinancialStatement
union all
select ticker, year, quarter, 'operationExpense' as financial_index, operationExpense as value
from Stage_FinancialStatement
union all
select ticker, year, quarter, 'operationProfit' as financial_index, operationProfit as value
from Stage_FinancialStatement
union all
select ticker, year, quarter, 'yearOperationProfitGrowth' as financial_index, yearOperationProfitGrowth as value
from Stage_FinancialStatement
union all
select ticker, year, quarter, 'quarterOperationProfitGrowth' as financial_index, quarterOperationProfitGrowth as value
from Stage_FinancialStatement
union all
select ticker, year, quarter, 'interestExpense' as financial_index, interestExpense as value
from Stage_FinancialStatement
union all
select ticker, year, quarter, 'preTaxProfit' as financial_index, preTaxProfit as value
from Stage_FinancialStatement
union all
select ticker, year, quarter, 'postTaxProfit' as financial_index, postTaxProfit as value
from Stage_FinancialStatement
union all
select ticker, year, quarter, 'shareHolderIncome' as financial_index, shareHolderIncome as value
from Stage_FinancialStatement
union all
select ticker, year, quarter, 'yearShareHolderIncomeGrowth' as financial_index, yearShareHolderIncomeGrowth as value
from Stage_FinancialStatement
union all
select ticker, year, quarter, 'quarterShareHolderIncomeGrowth' as financial_index, quarterShareHolderIncomeGrowth as value
from Stage_FinancialStatement
union all
select ticker, year, quarter, 'ebitda' as financial_index, ebitda as value
from Stage_FinancialStatement
union all
select ticker, year, quarter, 'investCost' as financial_index, investCost as value
from Stage_FinancialStatement
union all
select ticker, year, quarter, 'fromInvest' as financial_index, fromInvest as value
from Stage_FinancialStatement
union all
select ticker, year, quarter, 'fromFinancial' as financial_index, fromFinancial as value
from Stage_FinancialStatement
union all
select ticker, year, quarter, 'fromSale' as financial_index, fromSale as value
from Stage_FinancialStatement
union all
select ticker, year, quarter, 'freeCashFlow' as financial_index, freeCashFlow as value
from Stage_FinancialStatement;

-- unpivot ratio to Fact_FinancialRatio
create table Fact_FinancialRatio as
select ticker, year, quarter, 'priceToEarning' as financial_index, priceToEarning as value
from ratio
union all 
select ticker, year, quarter, 'priceToBook' as financial_index, priceToBook as value
from ratio
union all 
select ticker, year, quarter, 'valueBeforeEbitda' as financial_index, valueBeforeEbitda as value
from ratio
union all 
select ticker, year, quarter, 'roe' as financial_index, roe as value
from ratio
union all 
select ticker, year, quarter, 'roa' as financial_index, roa as value
from ratio
union all 
select ticker, year, quarter, 'daysReceivable' as financial_index, daysReceivable as value
from ratio
union all 
select ticker, year, quarter, 'daysInventory' as financial_index, daysInventory as value
from ratio
union all 
select ticker, year, quarter, 'daysPayable' as financial_index, daysPayable as value
from ratio
union all 
select ticker, year, quarter, 'ebitOnInterest' as financial_index, ebitOnInterest as value
from ratio
union all 
select ticker, year, quarter, 'earningPerShare' as financial_index, earningPerShare as value
from ratio
union all 
select ticker, year, quarter, 'bookValuePerShare' as financial_index, bookValuePerShare as value
from ratio
union all 
select ticker, year, quarter, 'equityOnTotalAsset' as financial_index, equityOnTotalAsset as value
from ratio
union all 
select ticker, year, quarter, 'equityOnLiability' as financial_index, equityOnLiability as value
from ratio
union all 
select ticker, year, quarter, 'currentPayment' as financial_index, currentPayment as value
from ratio
union all 
select ticker, year, quarter, 'quickPayment' as financial_index, quickPayment as value
from ratio
union all 
select ticker, year, quarter, 'epsChange' as financial_index, epsChange as value
from ratio
union all 
select ticker, year, quarter, 'ebitdaOnStock' as financial_index, ebitdaOnStock as value
from ratio
union all 
select ticker, year, quarter, 'grossProfitMargin' as financial_index, grossProfitMargin as value
from ratio
union all 
select ticker, year, quarter, 'operatingProfitMargin' as financial_index, operatingProfitMargin as value
from ratio
union all 
select ticker, year, quarter, 'postTaxMargin' as financial_index, postTaxMargin as value
from ratio
union all 
select ticker, year, quarter, 'debtOnEquity' as financial_index, debtOnEquity as value
from ratio
union all 
select ticker, year, quarter, 'debtOnAsset' as financial_index, debtOnAsset as value
from ratio
union all 
select ticker, year, quarter, 'debtOnEbitda' as financial_index, debtOnEbitda as value
from ratio
union all 
select ticker, year, quarter, 'shortOnLongDebt' as financial_index, shortOnLongDebt as value
from ratio
union all 
select ticker, year, quarter, 'assetOnEquity' as financial_index, assetOnEquity as value
from ratio
union all 
select ticker, year, quarter, 'capitalBalance' as financial_index, capitalBalance as value
from ratio
union all 
select ticker, year, quarter, 'cashOnEquity' as financial_index, cashOnEquity as value
from ratio
union all 
select ticker, year, quarter, 'cashOnCapitalize' as financial_index, cashOnCapitalize as value
from ratio
union all 
select ticker, year, quarter, 'cashCirculation' as financial_index, cashCirculation as value
from ratio
union all 
select ticker, year, quarter, 'revenueOnWorkCapital' as financial_index, revenueOnWorkCapital as value
from ratio
union all 
select ticker, year, quarter, 'capexOnFixedAsset' as financial_index, capexOnFixedAsset as value
from ratio
union all 
select ticker, year, quarter, 'revenueOnAsset' as financial_index, revenueOnAsset as value
from ratio
union all 
select ticker, year, quarter, 'postTaxOnPreTax' as financial_index, postTaxOnPreTax as value
from ratio
union all 
select ticker, year, quarter, 'ebitOnRevenue' as financial_index, ebitOnRevenue as value
from ratio
union all 
select ticker, year, quarter, 'preTaxOnEbit' as financial_index, preTaxOnEbit as value
from ratio
union all 
select ticker, year, quarter, 'payableOnEquity' as financial_index, payableOnEquity as value
from ratio
union all 
select ticker, year, quarter, 'ebitdaOnStockChange' as financial_index, ebitdaOnStockChange as value
from ratio
union all 
select ticker, year, quarter, 'bookValuePerShareChange' as financial_index, bookValuePerShareChange as value
from ratio;

-- create table dim_date
-- Tạo bảng dim_date
CREATE TABLE dim_date (
    time DATE PRIMARY KEY,
    year INT,
    month INT,
    quarter INT
);

-- Điền dữ liệu vào bảng dim_date
INSERT INTO dim_date (time, year, month, quarter)
SELECT
    date,
    YEAR(date),
    MONTH(date),
    QUARTER(date)
FROM (
    SELECT
        '2002-01-01' + INTERVAL seq DAY AS date
    FROM
        (SELECT @row := @row + 1 AS seq
        FROM (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) t1,
             (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) t2,
             (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) t3,
             (SELECT @row := -1) r
        ) seq
) dates
WHERE date BETWEEN '2002-01-01' AND '2024-12-31';

-- create table dim_year
CREATE TABLE dim_year (
    year INT PRIMARY KEY
);
-- Điền dữ liệu vào bảng dim_year
INSERT INTO dim_year (year) values (2002),(2003),(2004),(2005),(2006),(2007),(2008),(2009),(2010),(2011),(2012),(2013),(2014),(2015),(2016),(2017),(2018),(2019),(2020),(2021),(2022),(2023),(2024);

-- Create table dim_quarter
CREATE TABLE dim_quarter (
    quarter INT PRIMARY KEY
);
insert into dim_quarter (quarter) values (1),(2),(3),(4)