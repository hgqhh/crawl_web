-- Create Table company
CREATE TABLE company (
    ticker NVARCHAR(10) ,
    comGroupCode NVARCHAR(20),
    organName NVARCHAR(100),
    organShortName NVARCHAR(100),
    organTypeCode NVARCHAR(10),
    comTypeCode NVARCHAR(10),
    icbName NVARCHAR(100),
    icbNamePath nvarchar(100),
    sector NVARCHAR(50),
    industry NVARCHAR(50),
    `group` NVARCHAR(50),
    subgroup NVARCHAR(50),
    icbCode NVARCHAR(10)
);

-- Create Table balance_sheet
CREATE TABLE balance_sheet (
    ticker VARCHAR(10),
    shortAsset DECIMAL(20, 2),
    cash DECIMAL(20, 2),
    shortInvest DECIMAL(20, 2),
    shortReceivable DECIMAL(20, 2),
    inventory DECIMAL(20, 2),
    longAsset DECIMAL(20, 2),
    fixedAsset DECIMAL(20, 2),
    asset DECIMAL(20, 2),
    debt DECIMAL(20, 2),
    shortDebt DECIMAL(20, 2),
    longDebt DECIMAL(20, 2),
    equity DECIMAL(20, 2),
    capital DECIMAL(20, 2),
    centralBankDeposit DECIMAL(20, 2),
    otherBankDeposit DECIMAL(20, 2),
    otherBankLoan DECIMAL(20, 2),
    stockInvest DECIMAL(20, 2),
    customerLoan DECIMAL(20, 2),
    badLoan DECIMAL(20, 2),
    provision DECIMAL(20, 2),
    netCustomerLoan DECIMAL(20, 2),
    otherAsset DECIMAL(20, 2),
    otherBankCredit DECIMAL(20, 2),
    oweOtherBank DECIMAL(20, 2),
    oweCentralBank DECIMAL(20, 2),
    valuablePaper DECIMAL(20, 2),
    payableInterest DECIMAL(20, 2),
    receivableInterest DECIMAL(20, 2),
    deposit DECIMAL(20, 2),
    otherDebt DECIMAL(20, 2),
    fund DECIMAL(20, 2),
    unDistributedIncome DECIMAL(20, 2),
    minorShareHolderProfit DECIMAL(20, 2),
    payable DECIMAL(20, 2),
    year INT,
    quarter INT
);

-- Create Table cash_flow
CREATE TABLE cash_flow (
    ticker VARCHAR(10),
    investCost DECIMAL(20, 2),
    fromInvest DECIMAL(20, 2),
    fromFinancial DECIMAL(20, 2),
    fromSale DECIMAL(20, 2),
    freeCashFlow DECIMAL(20, 2),
    year INT
);

-- Create Table Income
CREATE TABLE income (
    ticker VARCHAR(10),
    revenue DECIMAL(20, 2),
    yearRevenueGrowth DECIMAL(5, 2),
    quarterRevenueGrowth DECIMAL(5, 2),
    costOfGoodSold DECIMAL(20, 2),
    grossProfit DECIMAL(20, 2),
    operationExpense DECIMAL(20, 2),
    operationProfit DECIMAL(20, 2),
    yearOperationProfitGrowth DECIMAL(5, 2),
    quarterOperationProfitGrowth DECIMAL(5, 2),
    interestExpense DECIMAL(20, 2),
    preTaxProfit DECIMAL(20, 2),
    postTaxProfit DECIMAL(20, 2),
    shareHolderIncome DECIMAL(20, 2),
    yearShareHolderIncomeGrowth DECIMAL(5, 2),
    quarterShareHolderIncomeGrowth DECIMAL(5, 2),
    investProfit DECIMAL(20, 2),
    serviceProfit DECIMAL(20, 2),
    otherProfit DECIMAL(20, 2),
    provisionExpense DECIMAL(20, 2),
    operationIncome DECIMAL(20, 2),
    ebitda DECIMAL(20, 2),
    year INT
);

-- Create table price_stock
CREATE TABLE price_stock (
    time DATETIME,
    open DECIMAL(20, 2),
    high DECIMAL(20, 2),
    low DECIMAL(20, 2),
    close DECIMAL(20, 2),
    volume BIGINT,
    ticker VARCHAR(10)
);
INSERT INTO price_stock (time, open, high, low, close, volume, ticker)
VALUES ('2012-03-20', 2800, 2850, 2800, 2800, 28300, 'CKV');

-- Create Table Ratio
CREATE TABLE ratio (
    `range` VARCHAR(20),
    ticker VARCHAR(10),
    quarter nvarchar(10),
    year INT,
    priceToEarning DECIMAL(10, 2),
    priceToBook DECIMAL(10, 2),
    valueBeforeEbitda DECIMAL(20, 2),
    roe DECIMAL(5, 2),
    roa DECIMAL(5, 2),
    daysReceivable INT,
    daysInventory INT,
    daysPayable INT,
    ebitOnInterest DECIMAL(10, 2),
    earningPerShare DECIMAL(20, 2),
    bookValuePerShare DECIMAL(20, 2),
    equityOnTotalAsset DECIMAL(5, 2),
    equityOnLiability DECIMAL(5, 2),
    currentPayment DECIMAL(5, 2),
    quickPayment DECIMAL(5, 2),
    epsChange DECIMAL(5, 2),
    ebitdaOnStock DECIMAL(10, 2),
    grossProfitMargin DECIMAL(5, 2),
    operatingProfitMargin DECIMAL(5, 2),
    postTaxMargin DECIMAL(5, 2),
    debtOnEquity DECIMAL(5, 2),
    debtOnAsset DECIMAL(5, 2),
    debtOnEbitda DECIMAL(10, 2),
    shortOnLongDebt DECIMAL(10, 2),
    assetOnEquity DECIMAL(10, 2),
    capitalBalance DECIMAL(20, 2),
    cashOnEquity DECIMAL(5, 2),
    cashOnCapitalize DECIMAL(5, 2),
    cashCirculation DECIMAL(20, 2),
    revenueOnWorkCapital DECIMAL(10, 2),
    capexOnFixedAsset DECIMAL(10, 2),
    revenueOnAsset DECIMAL(10, 2),
    postTaxOnPreTax DECIMAL(5, 2),
    ebitOnRevenue DECIMAL(5, 2),
    preTaxOnEbit DECIMAL(5, 2),
    payableOnEquity DECIMAL(5, 2),
    ebitdaOnStockChange DECIMAL(10, 2),
    bookValuePerShareChange DECIMAL(5, 2),
    PRIMARY KEY (ticker, year, quarter)
);



