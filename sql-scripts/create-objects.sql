CREATE TABLE "JSAO_FILES" (
   "FILE_NAME" VARCHAR2(255 BYTE) NOT NULL ENABLE,
   "BLOB_DATA" BLOB,
   "CONTENT_TYPE" VARCHAR2(50 BYTE) NOT NULL ENABLE,
   CONSTRAINT "JSAO_FILES_UK1" UNIQUE ("FILE_NAME")
);

create or replace PACKAGE jsao_file_load
AS

PROCEDURE upsert_file_chunk(
   p_file_name    IN VARCHAR2,
   p_content_type IN VARCHAR2,
   p_chunk_number IN PLS_INTEGER,
   p_piece_count  IN PLS_INTEGER,
   p_b64_piece_1  IN VARCHAR2,
   p_b64_piece_2  IN VARCHAR2 := NULL,
   p_b64_piece_3  IN VARCHAR2 := NULL,
   p_b64_piece_4  IN VARCHAR2 := NULL,
   p_b64_piece_5  IN VARCHAR2 := NULL,
   p_b64_piece_6  IN VARCHAR2 := NULL,
   p_b64_piece_7  IN VARCHAR2 := NULL,
   p_b64_piece_8  IN VARCHAR2 := NULL,
   p_b64_piece_9  IN VARCHAR2 := NULL,
   p_b64_piece_10 IN VARCHAR2 := NULL,
   p_b64_piece_11 IN VARCHAR2 := NULL,
   p_b64_piece_12 IN VARCHAR2 := NULL,
   p_b64_piece_13 IN VARCHAR2 := NULL,
   p_b64_piece_14 IN VARCHAR2 := NULL,
   p_b64_piece_15 IN VARCHAR2 := NULL,
   p_b64_piece_16 IN VARCHAR2 := NULL,
   p_b64_piece_17 IN VARCHAR2 := NULL,
   p_b64_piece_18 IN VARCHAR2 := NULL,
   p_b64_piece_19 IN VARCHAR2 := NULL,
   p_b64_piece_20 IN VARCHAR2 := NULL
);

PROCEDURE get_file_chunk(
   p_file_name      IN  VARCHAR2,
   p_byte_offset    IN  PLS_INTEGER,
   p_bytes_to_fetch IN  PLS_INTEGER,
   p_file_size      OUT PLS_INTEGER,
   p_bytes_fetched  OUT PLS_INTEGER,
   p_content_type   OUT VARCHAR2,
   p_piece_count    OUT PLS_INTEGER,
   p_b64_piece_1    OUT VARCHAR2,
   p_b64_piece_2    OUT VARCHAR2,
   p_b64_piece_3    OUT VARCHAR2,
   p_b64_piece_4    OUT VARCHAR2,
   p_b64_piece_5    OUT VARCHAR2,
   p_b64_piece_6    OUT VARCHAR2,
   p_b64_piece_7    OUT VARCHAR2,
   p_b64_piece_8    OUT VARCHAR2,
   p_b64_piece_9    OUT VARCHAR2,
   p_b64_piece_10   OUT VARCHAR2,
   p_b64_piece_11   OUT VARCHAR2,
   p_b64_piece_12   OUT VARCHAR2,
   p_b64_piece_13   OUT VARCHAR2,
   p_b64_piece_14   OUT VARCHAR2,
   p_b64_piece_15   OUT VARCHAR2,
   p_b64_piece_16   OUT VARCHAR2,
   p_b64_piece_17   OUT VARCHAR2,
   p_b64_piece_18   OUT VARCHAR2,
   p_b64_piece_19   OUT VARCHAR2,
   p_b64_piece_20   OUT VARCHAR2
);

END jsao_file_load;
/

create or replace PACKAGE BODY jsao_file_load
AS

PROCEDURE upsert_file_chunk(
   p_file_name    IN VARCHAR2,
   p_content_type IN VARCHAR2,
   p_chunk_number IN PLS_INTEGER,
   p_piece_count  IN PLS_INTEGER,
   p_b64_piece_1  IN VARCHAR2,
   p_b64_piece_2  IN VARCHAR2 := NULL,
   p_b64_piece_3  IN VARCHAR2 := NULL,
   p_b64_piece_4  IN VARCHAR2 := NULL,
   p_b64_piece_5  IN VARCHAR2 := NULL,
   p_b64_piece_6  IN VARCHAR2 := NULL,
   p_b64_piece_7  IN VARCHAR2 := NULL,
   p_b64_piece_8  IN VARCHAR2 := NULL,
   p_b64_piece_9  IN VARCHAR2 := NULL,
   p_b64_piece_10 IN VARCHAR2 := NULL,
   p_b64_piece_11 IN VARCHAR2 := NULL,
   p_b64_piece_12 IN VARCHAR2 := NULL,
   p_b64_piece_13 IN VARCHAR2 := NULL,
   p_b64_piece_14 IN VARCHAR2 := NULL,
   p_b64_piece_15 IN VARCHAR2 := NULL,
   p_b64_piece_16 IN VARCHAR2 := NULL,
   p_b64_piece_17 IN VARCHAR2 := NULL,
   p_b64_piece_18 IN VARCHAR2 := NULL,
   p_b64_piece_19 IN VARCHAR2 := NULL,
   p_b64_piece_20 IN VARCHAR2 := NULL
)

IS

   l_current_blob BLOB;
   l_raw_chunk    BLOB;

   TYPE vc2_aat IS TABLE OF VARCHAR2(32676)
      INDEX BY PLS_INTEGER;

   l_b64_pieces VC2_AAT;

BEGIN

   dbms_lob.createtemporary(
      lob_loc => l_raw_chunk,
      cache   => TRUE,
      dur     => dbms_lob.session
   );

   l_b64_pieces(1) := p_b64_piece_1;
   l_b64_pieces(2) := p_b64_piece_2;
   l_b64_pieces(3) := p_b64_piece_3;
   l_b64_pieces(4) := p_b64_piece_4;
   l_b64_pieces(5) := p_b64_piece_5;
   l_b64_pieces(6) := p_b64_piece_6;
   l_b64_pieces(7) := p_b64_piece_7;
   l_b64_pieces(8) := p_b64_piece_8;
   l_b64_pieces(9) := p_b64_piece_9;
   l_b64_pieces(10) := p_b64_piece_10;
   l_b64_pieces(11) := p_b64_piece_11;
   l_b64_pieces(12) := p_b64_piece_12;
   l_b64_pieces(13) := p_b64_piece_13;
   l_b64_pieces(14) := p_b64_piece_14;
   l_b64_pieces(15) := p_b64_piece_15;
   l_b64_pieces(16) := p_b64_piece_16;
   l_b64_pieces(17) := p_b64_piece_17;
   l_b64_pieces(18) := p_b64_piece_18;
   l_b64_pieces(19) := p_b64_piece_19;
   l_b64_pieces(20) := p_b64_piece_20;

   FOR x IN 1 .. p_piece_count
   LOOP
      dbms_lob.append(l_raw_chunk, utl_encode.base64_decode(utl_raw.cast_to_raw(l_b64_pieces(x))));
   END LOOP;

   IF p_chunk_number = 1
   THEN
      DELETE FROM jsao_files
      WHERE file_name = p_file_name;

      INSERT INTO jsao_files (
         file_name,
         content_type,
         blob_data
      ) VALUES (
         p_file_name,
         NVL(p_content_type, 'application/octet-stream'),
         l_raw_chunk
      );
   ELSE
      SELECT blob_data
      INTO l_current_blob
      FROM jsao_files
      WHERE file_name = p_file_name
      FOR UPDATE OF blob_data;

      dbms_lob.append(l_current_blob, l_raw_chunk);
   END IF;

END upsert_file_chunk;

PROCEDURE get_file_chunk(
   p_file_name      IN  VARCHAR2,
   p_byte_offset    IN  PLS_INTEGER,
   p_bytes_to_fetch IN  PLS_INTEGER,
   p_file_size      OUT PLS_INTEGER,
   p_bytes_fetched  OUT PLS_INTEGER,
   p_content_type   OUT VARCHAR2,
   p_piece_count    OUT PLS_INTEGER,
   p_b64_piece_1    OUT VARCHAR2,
   p_b64_piece_2    OUT VARCHAR2,
   p_b64_piece_3    OUT VARCHAR2,
   p_b64_piece_4    OUT VARCHAR2,
   p_b64_piece_5    OUT VARCHAR2,
   p_b64_piece_6    OUT VARCHAR2,
   p_b64_piece_7    OUT VARCHAR2,
   p_b64_piece_8    OUT VARCHAR2,
   p_b64_piece_9    OUT VARCHAR2,
   p_b64_piece_10   OUT VARCHAR2,
   p_b64_piece_11   OUT VARCHAR2,
   p_b64_piece_12   OUT VARCHAR2,
   p_b64_piece_13   OUT VARCHAR2,
   p_b64_piece_14   OUT VARCHAR2,
   p_b64_piece_15   OUT VARCHAR2,
   p_b64_piece_16   OUT VARCHAR2,
   p_b64_piece_17   OUT VARCHAR2,
   p_b64_piece_18   OUT VARCHAR2,
   p_b64_piece_19   OUT VARCHAR2,
   p_b64_piece_20   OUT VARCHAR2
)

IS

   l_max_piece_length PLS_INTEGER := 20000; --Before base64 encoding
   l_file_rec         JSAO_FILES%ROWTYPE;
   l_blob_length      PLS_INTEGER;
   l_bytes            INTEGER;
   l_raw_chunk        BLOB;
   l_more_to_read     BOOLEAN := TRUE;

   PROCEDURE read_piece
   IS
   BEGIN
      l_bytes := l_max_piece_length;

      dbms_lob.read(l_file_rec.blob_data, l_bytes, p_byte_offset + p_bytes_fetched, l_raw_chunk);

      p_bytes_fetched := p_bytes_fetched + l_bytes;
      p_piece_count := p_piece_count + 1;

      CASE p_piece_count
         WHEN 1
         THEN p_b64_piece_1 := utl_raw.cast_to_varchar2(utl_encode.base64_encode(l_raw_chunk));
         WHEN 2
         THEN p_b64_piece_2 := utl_raw.cast_to_varchar2(utl_encode.base64_encode(l_raw_chunk));
         WHEN 3
         THEN p_b64_piece_3 := utl_raw.cast_to_varchar2(utl_encode.base64_encode(l_raw_chunk));
         WHEN 4
         THEN p_b64_piece_4 := utl_raw.cast_to_varchar2(utl_encode.base64_encode(l_raw_chunk));
         WHEN 5
         THEN p_b64_piece_5 := utl_raw.cast_to_varchar2(utl_encode.base64_encode(l_raw_chunk));
         WHEN 6
         THEN p_b64_piece_6 := utl_raw.cast_to_varchar2(utl_encode.base64_encode(l_raw_chunk));
         WHEN 7
         THEN p_b64_piece_7 := utl_raw.cast_to_varchar2(utl_encode.base64_encode(l_raw_chunk));
         WHEN 8
         THEN p_b64_piece_8 := utl_raw.cast_to_varchar2(utl_encode.base64_encode(l_raw_chunk));
         WHEN 9
         THEN p_b64_piece_9 := utl_raw.cast_to_varchar2(utl_encode.base64_encode(l_raw_chunk));
         WHEN 10
         THEN p_b64_piece_10 := utl_raw.cast_to_varchar2(utl_encode.base64_encode(l_raw_chunk));
         WHEN 11
         THEN p_b64_piece_11 := utl_raw.cast_to_varchar2(utl_encode.base64_encode(l_raw_chunk));
         WHEN 12
         THEN p_b64_piece_12 := utl_raw.cast_to_varchar2(utl_encode.base64_encode(l_raw_chunk));
         WHEN 13
         THEN p_b64_piece_13 := utl_raw.cast_to_varchar2(utl_encode.base64_encode(l_raw_chunk));
         WHEN 14
         THEN p_b64_piece_14 := utl_raw.cast_to_varchar2(utl_encode.base64_encode(l_raw_chunk));
         WHEN 15
         THEN p_b64_piece_15 := utl_raw.cast_to_varchar2(utl_encode.base64_encode(l_raw_chunk));
         WHEN 16
         THEN p_b64_piece_16 := utl_raw.cast_to_varchar2(utl_encode.base64_encode(l_raw_chunk));
         WHEN 17
         THEN p_b64_piece_17 := utl_raw.cast_to_varchar2(utl_encode.base64_encode(l_raw_chunk));
         WHEN 18
         THEN p_b64_piece_18 := utl_raw.cast_to_varchar2(utl_encode.base64_encode(l_raw_chunk));
         WHEN 19
         THEN p_b64_piece_19 := utl_raw.cast_to_varchar2(utl_encode.base64_encode(l_raw_chunk));
         WHEN 20
         THEN p_b64_piece_20 := utl_raw.cast_to_varchar2(utl_encode.base64_encode(l_raw_chunk));
      END CASE;

      IF p_bytes_fetched < p_bytes_to_fetch
      THEN
         l_more_to_read := FALSE;
      ELSE
         l_more_to_read := TRUE;
      END IF;
   END;

BEGIN

   p_bytes_fetched := 0;
   p_piece_count := 0;

   dbms_lob.createtemporary(
      lob_loc => l_raw_chunk,
      cache   => TRUE,
      dur     => dbms_lob.session
   );

   SELECT file_name,
      blob_data,
      content_type
   INTO l_file_rec
   FROM jsao_files
   WHERE file_name = p_file_name;

   l_blob_length := dbms_lob.getlength(l_file_rec.blob_data);
   p_file_size :=  l_blob_length;

   p_content_type := l_file_rec.content_type;

   WHILE l_more_to_read
   LOOP
      read_piece;
   END LOOP;

END get_file_chunk;

END jsao_file_load;
/