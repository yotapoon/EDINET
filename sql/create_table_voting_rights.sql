DROP TABLE IF EXISTS EDINET.dbo.VotingRights;

CREATE TABLE EDINET.dbo.VotingRights(
    docId CHAR(8) NOT NULL,
    seqNumber INT NOT NULL,
    SubmissionDate DATE,
    FiscalPeriodEnd DATE,
    SecuritiesCode CHAR(5),
    TotalNumberOfIssuedShares DECIMAL(20, 0),
    NumberOfOtherSharesWithFullVotingRights DECIMAL(20, 0),
    NumberOfTreasurySharesWithFullVotingRights DECIMAL(20, 0),
    NumberOfSharesLessThanOneUnit DECIMAL(20, 0),
    PRIMARY KEY (docId, seqNumber)
);

--SELECT TOP 100 * FROM EDINET.dbo.VotingRights