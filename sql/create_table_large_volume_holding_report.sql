DROP TABLE IF EXISTS EDINET.dbo.LargeVolumeHoldingReport;

CREATE TABLE EDINET.dbo.LargeVolumeHoldingReport(
    docId CHAR(8) NOT NULL,
    seqNumber INT,
    member INT NOT NULL,
    submitterName NVARCHAR(MAX),
    submitterEdinetCode CHAR(6),
    dateFile DATE,
    obligationDate DATE,
    isAmendment BIT,
    submissionCount INT,
    issuerSecurityCode CHAR(5),
    issuerName NVARCHAR(MAX),
    holderEdinetCode CHAR(6),
    holderName NVARCHAR(MAX),
    holdingPurpose NVARCHAR(MAX),
    importantProposal NVARCHAR(MAX),
    baseDate DATE,
    totalOutstandingShares DECIMAL(20, 0),
    totalSharesHeld DECIMAL(20, 0),
    holdingRatio DECIMAL(8, 5),
    previousHoldingRatio DECIMAL(8, 5),
    ownFunds DECIMAL(20, 0),
    totalBorrowings DECIMAL(20, 0),
    otherFunds DECIMAL(20, 0),
    totalAcquisitionFunds DECIMAL(20, 0),
    PRIMARY KEY (docId, member)
);


SELECT
    *
FROM
    EDINET.dbo.LargeVolumeHoldingReport
WHERE
    submitterName LIKE N'%–ì‘º%'
ORDER BY
    dateFile DESC;