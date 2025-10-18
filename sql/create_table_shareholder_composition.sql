DROP TABLE IF EXISTS EDINET.dbo.ShareholderComposition;

CREATE TABLE EDINET.dbo.ShareholderComposition(
    docId CHAR(8) NOT NULL,
    seqNumber INT NOT NULL,
    SubmissionDate DATE,
    FiscalPeriodEnd DATE,
    SecuritiesCode CHAR(5),
    Category NVARCHAR(255) NOT NULL,
    NumberOfShareholders BIGINT,
    PercentageOfShareholdings DECIMAL(8, 5),
    NumberOfSharesHeldUnits DECIMAL(20, 0),
    PRIMARY KEY (docId, seqNumber, Category)
);
