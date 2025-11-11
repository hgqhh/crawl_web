-- Trigger sau khi thêm dữ liệu vào bảng cashflow (vì bảng cashflow sẽ được insert cuối nên trigger này sẽ thực hiện sau khi bảng cashflow được thêm)
CREATE TRIGGER trg_cashflow_after_insert
AFTER INSERT ON cashflow
FOR EACH ROW
BEGIN
    CALL UpdateFactFinancialStatement();
END;

-- Trigger UpdateFactFinancialRatio sau khi bảng ratio được thêm

CREATE TRIGGER trg_ratio_after_insert
AFTER INSERT ON ratio
FOR EACH ROW
BEGIN
    CALL UpdateFactFinancialRatio();
END;
