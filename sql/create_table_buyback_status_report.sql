DROP TABLE IF EXISTS EDINET.dbo.BuybackStatusReport;

CREATE TABLE EDINET.dbo.BuybackStatusReport(
    docID CHAR(8) NOT NULL,
    dateFile DATE NOT NULL,
    seqNumber INT NOT NULL,
    secCode CHAR(5),
    ordinanceCode CHAR(3),
    formCode CHAR(6),
    acquisitionStatus TEXT,
    disposalStatus TEXT,
    holdingStatus TEXT,
    PRIMARY KEY (docID, dateFile, seqNumber)
);


--SELECT * FROM EDINET.dbo.BuybackStatusReport ORDER BY dateFile desc

SELECT
    *
FROM
    EDINET.dbo.BuybackStatusReport
WHERE
    secCode = '80580'
ORDER BY dateFile desc