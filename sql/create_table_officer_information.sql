DROP TABLE IF EXISTS EDINET.dbo.OfficerInformation;

CREATE TABLE EDINET.dbo.OfficerInformation(
    docId CHAR(8) NOT NULL,
    seqNumber INT NOT NULL,
    SubmissionDate DATE,
    FiscalPeriodEnd DATE,
    SecuritiesCode CHAR(5),
    officerId NVARCHAR(255) NOT NULL,
    Name NVARCHAR(MAX),
    IsNewAppointment BIT,
    DateOfBirth DATE,
    Title NVARCHAR(MAX),
    NumberOfSharesHeld DECIMAL(20, 0),
    TotalRemuneration DECIMAL(20, 0),
    TermOfOffice NVARCHAR(MAX),
    CareerSummary NVARCHAR(MAX),
    PRIMARY KEY (docId, seqNumber, officerId)
);
