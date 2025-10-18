DROP TABLE IF EXISTS EDINET.dbo.SpecifiedInvestment;

CREATE TABLE EDINET.dbo.SpecifiedInvestment(
    docId CHAR(8) NOT NULL,
    seqNumber INT NOT NULL,
    SubmissionDate DATE,
    FiscalPeriodEnd DATE,
    SecuritiesCode CHAR(5),
    HoldingEntity NVARCHAR(255) NOT NULL,
    rowId INT NOT NULL,
    NameOfSecurities NVARCHAR(MAX) NOT NULL,
    NumberOfSharesHeldCurrentYear DECIMAL(20, 0),
    BookValueCurrentYear DECIMAL(20, 0),
    NumberOfSharesHeldPriorYear DECIMAL(20, 0),
    BookValuePriorYear DECIMAL(20, 0),
    HoldingPurpose NVARCHAR(MAX),
    CrossShareholdingStatus NVARCHAR(MAX),
    PRIMARY KEY (docId, seqNumber, HoldingEntity, rowId)
);
