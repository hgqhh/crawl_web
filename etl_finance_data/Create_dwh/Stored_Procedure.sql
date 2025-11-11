-- Stored Procedure để update bảng Stage và Fact (chỉ insert dữ liệu mới)
DELIMITER //

CREATE PROCEDURE UpdateFactFinancialStatement()
BEGIN
    DECLARE max_year INT;
    DECLARE max_quarter INT;
    
    -- Bước 1: Lấy năm và quý mới nhất từ Stage_FinancialStatement
    SELECT COALESCE(MAX(year), 2010), COALESCE(MAX(quarter), 0) 
    INTO max_year, max_quarter
    FROM Stage_FinancialStatement;
    
    -- Bước 2: Insert chỉ dữ liệu mới vào Stage_FinancialStatement
    -- Chỉ lấy các record có (year > max_year) HOẶC (year = max_year VÀ quarter > max_quarter)
    INSERT INTO Stage_FinancialStatement
    SELECT 
        b.*,
        i.revenue, i.yearRevenueGrowth, i.quarterRevenueGrowth, i.costOfGoodSold, i.grossProfit, 
        i.operationExpense, i.operationProfit, i.yearOperationProfitGrowth, i.quarterOperationProfitGrowth, 
        i.interestExpense, i.preTaxProfit, i.postTaxProfit, i.shareHolderIncome, i.yearShareHolderIncomeGrowth, 
        i.quarterShareHolderIncomeGrowth, i.investProfit, i.serviceProfit, i.otherProfit, i.provisionExpense, 
        i.operationIncome, i.ebitda,
        cf.investCost, cf.fromInvest, cf.fromFinancial, cf.fromSale, cf.freeCashFlow
    FROM balance_sheet b 
    JOIN income i ON b.ticker = i.ticker AND b.year = i.year AND b.quarter = i.quarter
    JOIN cash_flow cf ON b.ticker = cf.ticker AND b.year = cf.year AND b.quarter = cf.quarter
    WHERE (b.year > max_year) OR (b.year = max_year AND b.quarter > max_quarter);
    
    -- Bước 3: Lấy năm và quý mới nhất từ Fact_FinancialStatement
    SELECT COALESCE(MAX(year), 2010), COALESCE(MAX(quarter), 0) 
    INTO max_year, max_quarter
    FROM Fact_FinancialStatement;
    
    -- Bước 4: Insert chỉ dữ liệu mới vào Fact_FinancialStatement (unpivot)
    -- Sử dụng CTE để tránh lặp lại subquery
    INSERT INTO Fact_FinancialStatement
    WITH NewData AS (
        SELECT * FROM Stage_FinancialStatement
        WHERE (year > max_year) OR (year = max_year AND quarter > max_quarter)
    )
    SELECT ticker, year, quarter, 'shortAsset' as financial_index, shortAsset as value FROM NewData
    UNION ALL
    SELECT ticker, year, quarter, 'shortInvest' as financial_index, shortInvest as value FROM NewData
    UNION ALL
    SELECT ticker, year, quarter, 'shortReceivable' as financial_index, shortReceivable as value FROM NewData
    UNION ALL
    SELECT ticker, year, quarter, 'inventory' as financial_index, inventory as value FROM NewData
    UNION ALL
    SELECT ticker, year, quarter, 'longAsset' as financial_index, longAsset as value FROM NewData
    UNION ALL
    SELECT ticker, year, quarter, 'fixedAsset' as financial_index, fixedAsset as value FROM NewData
    UNION ALL
    SELECT ticker, year, quarter, 'asset' as financial_index, asset as value FROM NewData
    UNION ALL
    SELECT ticker, year, quarter, 'debt' as financial_index, debt as value FROM NewData
    UNION ALL
    SELECT ticker, year, quarter, 'shortDebt' as financial_index, shortDebt as value FROM NewData
    UNION ALL
    SELECT ticker, year, quarter, 'longDebt' as financial_index, longDebt as value FROM NewData
    UNION ALL
    SELECT ticker, year, quarter, 'equity' as financial_index, equity as value FROM NewData
    UNION ALL
    SELECT ticker, year, quarter, 'capital' as financial_index, capital as value FROM NewData
    UNION ALL
    SELECT ticker, year, quarter, 'minorShareHolderProfit' as financial_index, minorShareHolderProfit as value FROM NewData
    UNION ALL
    SELECT ticker, year, quarter, 'payable' as financial_index, payable as value FROM NewData
    UNION ALL
    SELECT ticker, year, quarter, 'revenue' as financial_index, revenue as value FROM NewData
    UNION ALL
    SELECT ticker, year, quarter, 'yearRevenueGrowth' as financial_index, yearRevenueGrowth as value FROM NewData
    UNION ALL
    SELECT ticker, year, quarter, 'quarterRevenueGrowth' as financial_index, quarterRevenueGrowth as value FROM NewData
    UNION ALL
    SELECT ticker, year, quarter, 'costOfGoodSold' as financial_index, costOfGoodSold as value FROM NewData
    UNION ALL
    SELECT ticker, year, quarter, 'grossProfit' as financial_index, grossProfit as value FROM NewData
    UNION ALL
    SELECT ticker, year, quarter, 'operationExpense' as financial_index, operationExpense as value FROM NewData
    UNION ALL
    SELECT ticker, year, quarter, 'operationProfit' as financial_index, operationProfit as value FROM NewData
    UNION ALL
    SELECT ticker, year, quarter, 'yearOperationProfitGrowth' as financial_index, yearOperationProfitGrowth as value FROM NewData
    UNION ALL
    SELECT ticker, year, quarter, 'quarterOperationProfitGrowth' as financial_index, quarterOperationProfitGrowth as value FROM NewData
    UNION ALL
    SELECT ticker, year, quarter, 'interestExpense' as financial_index, interestExpense as value FROM NewData
    UNION ALL
    SELECT ticker, year, quarter, 'preTaxProfit' as financial_index, preTaxProfit as value FROM NewData
    UNION ALL
    SELECT ticker, year, quarter, 'postTaxProfit' as financial_index, postTaxProfit as value FROM NewData
    UNION ALL
    SELECT ticker, year, quarter, 'shareHolderIncome' as financial_index, shareHolderIncome as value FROM NewData
    UNION ALL
    SELECT ticker, year, quarter, 'yearShareHolderIncomeGrowth' as financial_index, yearShareHolderIncomeGrowth as value FROM NewData
    UNION ALL
    SELECT ticker, year, quarter, 'quarterShareHolderIncomeGrowth' as financial_index, quarterShareHolderIncomeGrowth as value FROM NewData
    UNION ALL
    SELECT ticker, year, quarter, 'ebitda' as financial_index, ebitda as value FROM NewData
    UNION ALL
    SELECT ticker, year, quarter, 'investCost' as financial_index, investCost as value FROM NewData
    UNION ALL
    SELECT ticker, year, quarter, 'fromInvest' as financial_index, fromInvest as value FROM NewData
    UNION ALL
    SELECT ticker, year, quarter, 'fromFinancial' as financial_index, fromFinancial as value FROM NewData
    UNION ALL
    SELECT ticker, year, quarter, 'fromSale' as financial_index, fromSale as value FROM NewData
    UNION ALL
    SELECT ticker, year, quarter, 'freeCashFlow' as financial_index, freeCashFlow as value FROM NewData;
    
END //

DELIMITER ;

-- Tạo trigger để tự động gọi stored procedure sau khi insert vào cashflow
DROP TRIGGER IF EXISTS trg_cashflow_after_insert;

DELIMITER //

CREATE TRIGGER trg_cashflow_after_insert
AFTER INSERT ON cash_flow
FOR EACH ROW
BEGIN
    CALL UpdateFactFinancialStatement();
END //

DELIMITER ;