DROP TABLE IF EXISTS EDINET.dbo.TenderOffer;

CREATE TABLE EDINET.dbo.TenderOffer(
    docId CHAR(8) NOT NULL,
    seqNumber INT NOT NULL,
    dateFile DATE,
    offererName NVARCHAR(MAX),
    targetName NVARCHAR(MAX),
    targetSecCode CHAR(5),
    offerPrice DECIMAL(20, 2),
    offerPeriodStart DATE,
    offerPeriodEnd DATE,
    minSharesToBuy DECIMAL(20, 0),
    maxSharesToBuy DECIMAL(20, 0),
    tenderedShares DECIMAL(20, 0),
    opinion NVARCHAR(MAX),
    reasonForOpinion NVARCHAR(MAX),
    PRIMARY KEY (docId, seqNumber)
);
