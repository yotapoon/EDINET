DROP TABLE IF EXISTS EDINET.dbo.MajorShareholders;

CREATE TABLE EDINET.dbo.MajorShareholders(
    docId CHAR(8) NOT NULL,
    seqNumber INT NOT NULL,
    SubmissionDate DATE,
    FiscalPeriodEnd DATE,
    SecuritiesCode CHAR(5),
    shareholderId INT NOT NULL,
    MajorShareholderName NVARCHAR(MAX) NOT NULL,
    VotingRightsRatio DECIMAL(8, 5),
    NumberOfSharesHeld DECIMAL(20, 0),
    PRIMARY KEY (docId, seqNumber, shareholderId)
);
