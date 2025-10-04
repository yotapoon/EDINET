
DROP TABLE IF EXISTS EDINET.dbo.Submission;

CREATE TABLE EDINET.dbo.Submission (
  dateFile DATE NOT NULL,
  seqNumber INT NULL,
  docID CHAR(8) NOT NULL,
  edinetCode CHAR(6) NULL,
  secCode CHAR(5) NULL,
  JCN CHAR(13) NULL,
  filerName NVARCHAR(128) NULL,
  fundCode CHAR(6) NULL,
  ordinanceCode CHAR(3) NULL,
  formCode CHAR(6) NULL,
  docTypeCode CHAR(3) NULL, -- 書類種別コード
  periodStart DATE NULL,
  periodEnd DATE NULL,
  submitDateTime DATETIME NULL,
  docDescription NVARCHAR(147) NULL,
  issuerEdinetCode CHAR(6) NULL,
  subjectEdinetCode CHAR(6) NULL,
  subsidiaryEdinetCode NVARCHAR(69) NULL,
  currentReportReason NVARCHAR(1000) NULL,
  parentDocID CHAR(8) NULL,
  opeDateTime DATETIME NULL,
  withdrawalStatus CHAR(1) NULL,
  docInfoEditStatus CHAR(1) NULL,
  disclosureStatus CHAR(1) NULL,
  xbrlFlag BIT NULL,
  pdfFlag BIT NULL,
  attachDocFlag BIT NULL,
  englishDocFlag BIT NULL,
  csvFlag BIT NULL,
  legalStatus CHAR(1) NULL,
  FlagLoadCsv BIT NULL

);

--ALTER TABLE EDINET.dbo.Submission ADD FlagLoadCsv BIT DEFAULT 0;
ALTER TABLE EDINET.dbo.Submission ADD CONSTRAINT PK_Submission PRIMARY KEY (dateFile, seqNumber, docID)


CREATE NONCLUSTERED INDEX IX_Submission_docID
  ON Edinet.dbo.Submission(docID)

-- 提出日による検索
CREATE NONCLUSTERED INDEX IX_Submission_dateFile
  ON Edinet.dbo.Submission(dateFile)
  --INCLUDE (seqNumber, docID);

-- EDINETコードによる検索
CREATE NONCLUSTERED INDEX IX_Submission_edinetCode
  ON Edinet.dbo.Submission(edinetCode)
  --INCLUDE (submitDateTime, docID, periodStart, periodEnd);

-- 証券コードによる検索
CREATE NONCLUSTERED INDEX IX_Submission_secCode
  ON Edinet.dbo.Submission(secCode)
  --INCLUDE (submitDateTime, docID, periodStart, periodEnd);

--SELECT TOP 100 * FROM EDINET.dbo.Submission ORDER BY DATEFILE DESC
--SELECT * FROM EDINET.dbo.Submission WHERE docID = 'S100WM4O'

/*
WITH DuplicatesCTE AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY docID -- Group rows by the same docID
            ORDER BY submitDateTime DESC, opeDateTime DESC -- Order by newest submission time within each group
        ) AS RowNum
    FROM
        EDINET.dbo.Submission
)
DELETE FROM DuplicatesCTE
WHERE RowNum > 1; -- Delete all but the first (newest) record in each group
*/