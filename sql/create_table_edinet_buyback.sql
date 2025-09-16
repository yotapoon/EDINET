
DROP TABLE IF EXISTS EDINET.dbo.Buyback;

CREATE TABLE EDINET.dbo.Buyback(
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
  docTypeCode CHAR(3) NULL, -- èëóﬁéÌï ÉRÅ[Éh
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

