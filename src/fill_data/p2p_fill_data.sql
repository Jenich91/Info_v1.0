call pr_p2p_insert('class','duck', 'C2_SimpleBashUtils', 'Start', '00:01:12');
call pr_p2p_insert('class','duck', 'C2_SimpleBashUtils','Success','00:02:16');
call pr_p2p_insert('class','duck', 'C3_Stringplus', 'Start', '00:03:16');
call pr_p2p_insert('class','duck', 'C3_Stringplus','Failure','00:04:16'); -- *

call pr_p2p_insert('class','duck', 'C4_Math', 'Start', '00:05:16');
call pr_p2p_insert('class','duck', 'C4_Math','Success','00:06:16');
call pr_p2p_insert('class','duck', 'C5_Decimal', 'Start', '00:07:12');
call pr_p2p_insert('class','duck', 'C5_Decimal','Success','00:08:16');
call pr_p2p_insert('class','duck', 'C6_Matrix', 'Start', '00:09:12');
call pr_p2p_insert('class','duck', 'C6_Matrix','Success','00:10:16');
call pr_p2p_insert('class','duck', 'C7_SmartCalc_v1', 'Start', '00:11:12');
call pr_p2p_insert('class','duck', 'C7_SmartCalc_v1','Success','00:12:16'); -- no verter
call pr_p2p_insert('class','duck', 'C8_3DViewer_v1', 'Start', '00:13:12');
call pr_p2p_insert('class','duck', 'C8_3DViewer_v1','Success','00:14:16'); -- no verter

call pr_p2p_insert('class','duck', 'CPP1_s21_matrix', 'Start', '00:15:12');
call pr_p2p_insert('class','duck', 'CPP1_s21_matrix','Success','00:16:16'); -- no verter

call pr_p2p_insert('class','duck', 'CPP2_s21_containers', 'Start', '00:17:12');
call pr_p2p_insert('class','duck', 'CPP2_s21_containers','Success','00:18:16'); -- no verter

call pr_p2p_insert('class','duck', 'CPP3_SmartCalc_v2', 'Start', '00:19:12');
call pr_p2p_insert('class','duck', 'CPP3_SmartCalc_v2','Success','00:20:16'); -- no verter

call pr_p2p_insert('class','duck', 'CPP4_3DViewer_v2', 'Start', '00:21:12');
call pr_p2p_insert('class','duck', 'CPP4_3DViewer_v2','Failure','00:22:16'); -- *

call pr_p2p_insert('class','duck', 'CPP4_3DViewer_v2', 'Start', '00:30:12');
call pr_p2p_insert('class','duck', 'CPP4_3DViewer_v2','Success','00:34:16'); -- no verter

-- ultra
call pr_p2p_insert('ultra', 'class', 'C2_SimpleBashUtils', 'Start', '01:01:12');
call pr_p2p_insert('ultra', 'class', 'C2_SimpleBashUtils','Success','01:02:12'); 

call pr_p2p_insert('ultra','class', 'C3_Stringplus', 'Start', '01:03:12');
call pr_p2p_insert('ultra','class', 'C3_Stringplus','Success','01:04:16'); --> verter fail

call pr_p2p_insert('ultra','class', 'C4_Math', 'Start', '01:05:12');
call pr_p2p_insert('ultra','class', 'C4_Math','Success','01:06:16');

--------------------------------------------------------------------------------

call pr_p2p_insert('ultra','class', 'C5_Decimal', 'Start', '01:07:12');
call pr_p2p_insert('ultra','class', 'C5_Decimal','Success','01:08:16');

call pr_p2p_insert('ultra','class', 'C6_Matrix', 'Start', '01:09:12');
call pr_p2p_insert('ultra','class', 'C6_Matrix','Success','01:10:16'); --> verter fail

call pr_p2p_insert('ultra','class', 'C7_SmartCalc_v1', 'Start', '01:11:12');
call pr_p2p_insert('ultra','class', 'C7_SmartCalc_v1','Success','01:12:16'); -- no verter

call pr_p2p_insert('ultra','class', 'C8_3DViewer_v1', 'Start', '01:13:12');
call pr_p2p_insert('ultra','class', 'C8_3DViewer_v1','Success','01:14:16'); -- no verter

call pr_p2p_insert('ultra','class', 'CPP1_s21_matrix', 'Start', '01:15:12');
call pr_p2p_insert('ultra','class', 'CPP1_s21_matrix','Success','01:16:16'); -- no verter

call pr_p2p_insert('ultra','class', 'CPP2_s21_containers', 'Start', '01:17:12');
call pr_p2p_insert('ultra','class', 'CPP2_s21_containers','Success','01:18:16'); -- no verter

call pr_p2p_insert('ultra','class', 'CPP3_SmartCalc_v2', 'Start', '01:19:12');
call pr_p2p_insert('ultra','class', 'CPP3_SmartCalc_v2','Success','01:20:16'); -- no verter

call pr_p2p_insert('ultra','class', 'CPP4_3DViewer_v2', 'Start', '01:21:12');
call pr_p2p_insert('ultra','class', 'CPP4_3DViewer_v2','Success','01:22:16'); -- no verter

call pr_p2p_insert('ultra','class', 'C8_3DViewer_v1', 'Start', '01:24:12');
call pr_p2p_insert('ultra','class', 'C8_3DViewer_v1','Failure','01:28:16'); -- *

-- adequate
call pr_p2p_insert('adequate','class', 'C2_SimpleBashUtils', 'Start', '02:01:12');
call pr_p2p_insert('adequate','class', 'C2_SimpleBashUtils','Failure','02:02:16'); -- *

call pr_p2p_insert('adequate','class', 'C3_Stringplus', 'Start', '02:03:12');
call pr_p2p_insert('adequate','class', 'C3_Stringplus','Success','02:04:16');

call pr_p2p_insert('adequate','class', 'C4_Math', 'Start', '02:05:12');
call pr_p2p_insert('adequate','class', 'C4_Math','Success','02:06:16');

call pr_p2p_insert('adequate','class', 'C5_Decimal', 'Start', '02:07:12');
call pr_p2p_insert('adequate','class', 'C5_Decimal','Success','02:08:16');

call pr_p2p_insert('adequate','class', 'C6_Matrix', 'Start', '02:09:12');
call pr_p2p_insert('adequate','class', 'C6_Matrix','Success','02:10:16'); --> verter fail

call pr_p2p_insert('adequate','class', 'C7_SmartCalc_v1', 'Start', '02:11:12');
call pr_p2p_insert('adequate','class', 'C7_SmartCalc_v1','Success','02:12:16'); -- no verter

--------------------------------------------------------------------------------

call pr_p2p_insert('adequate','class', 'C8_3DViewer_v1', 'Start', '02:13:12');
call pr_p2p_insert('adequate','class', 'C8_3DViewer_v1','Success','02:14:16'); -- no verter

call pr_p2p_insert('adequate','class', 'CPP1_s21_matrix', 'Start', '02:15:12');
call pr_p2p_insert('adequate','class', 'CPP1_s21_matrix','Success','02:16:16'); -- no verter

call pr_p2p_insert('adequate','class', 'CPP2_s21_containers', 'Start', '02:17:12');
call pr_p2p_insert('adequate','class', 'CPP2_s21_containers','Success','02:18:16'); -- no verter

call pr_p2p_insert('adequate','class', 'CPP3_SmartCalc_v2', 'Start', '02:19:12');
call pr_p2p_insert('adequate','class', 'CPP3_SmartCalc_v2','Success','02:20:16'); -- no verter

call pr_p2p_insert('adequate','class', 'CPP4_3DViewer_v2', 'Start', '02:21:12');
call pr_p2p_insert('adequate','class', 'CPP4_3DViewer_v2','Success','02:22:16'); -- no verter

call pr_p2p_insert('adequate','duck', 'D01_Linux', 'Start', '02:23:12');
call pr_p2p_insert('adequate','duck', 'D01_Linux','Success','02:24:16'); -- no verter

call pr_p2p_insert('adequate','class', 'C2_SimpleBashUtils', 'Start', '02:29:12');
call pr_p2p_insert('adequate','class', 'C2_SimpleBashUtils','Failure','02:36:16'); -- *

call pr_p2p_insert('adequate','class', 'C2_SimpleBashUtils', 'Start', '02:38:12');
call pr_p2p_insert('adequate','class', 'C2_SimpleBashUtils','Success','02:41:16');

call pr_p2p_insert('adequate','class', 'C3_Stringplus', 'Start', '02:44:12');
call pr_p2p_insert('adequate','class', 'C3_Stringplus','Success','02:58:12');

-- heel
call pr_p2p_insert('heel','adequate', 'C2_SimpleBashUtils', 'Start', '03:01:12');
call pr_p2p_insert('heel','adequate', 'C2_SimpleBashUtils','Success','03:02:16');

call pr_p2p_insert('heel','adequate', 'C3_Stringplus', 'Start', '03:03:12');
call pr_p2p_insert('heel','adequate', 'C3_Stringplus','Success','03:04:16');

call pr_p2p_insert('heel','adequate', 'C4_Math', 'Start', '03:05:12');
call pr_p2p_insert('heel','adequate', 'C4_Math','Success','03:06:16');

call pr_p2p_insert('heel','adequate', 'C5_Decimal', 'Start', '03:07:12');
call pr_p2p_insert('heel','adequate', 'C5_Decimal','Success','03:08:16'); -- verter fail

call pr_p2p_insert('heel','adequate', 'C6_Matrix', 'Start', '03:09:12');
call pr_p2p_insert('heel','adequate', 'C6_Matrix','Success','03:10:16');

call pr_p2p_insert('heel','adequate', 'C7_SmartCalc_v1', 'Start', '03:11:12');
call pr_p2p_insert('heel','adequate', 'C7_SmartCalc_v1','Success','03:12:16');  -- no verter

call pr_p2p_insert('heel','adequate', 'C8_3DViewer_v1', 'Start', '03:13:12');
call pr_p2p_insert('heel','adequate', 'C8_3DViewer_v1','Success','03:14:16');  -- no verter

call pr_p2p_insert('heel','adequate', 'CPP1_s21_matrix', 'Start', '03:15:12');
call pr_p2p_insert('heel','adequate', 'CPP1_s21_matrix','Success','03:16:16');  -- no verter

call pr_p2p_insert('heel','adequate', 'CPP2_s21_containers', 'Start', '03:17:12');
call pr_p2p_insert('heel','adequate', 'CPP2_s21_containers','Success','03:18:16');  -- no verter

call pr_p2p_insert('heel','duck', 'D01_Linux', 'Start', '03:19:12'); 
call pr_p2p_insert('heel','duck', 'D01_Linux','Success','03:20:16');  -- no verter

call pr_p2p_insert('heel','adequate', 'C4_Math', 'Start', '03:35:12');
call pr_p2p_insert('heel','adequate', 'C4_Math','Failure','03:36:16'); -- *

--------------------------------------------------------------------------------

call pr_p2p_insert('heel','adequate', 'C5_Decimal', 'Start', '03:40:12');
call pr_p2p_insert('heel','adequate', 'C5_Decimal','Failure','03:43:16'); -- *

call pr_p2p_insert('heel','adequate', 'C6_Matrix', 'Start', '03:48:12');
call pr_p2p_insert('heel','adequate', 'C6_Matrix','Failure','03:54:16'); -- *

-- transom
call pr_p2p_insert('transom','heel', 'C2_SimpleBashUtils', 'Start', '04:01:12');
call pr_p2p_insert('transom','heel', 'C2_SimpleBashUtils','Success','04:02:16');

call pr_p2p_insert('transom','heel', 'C3_Stringplus', 'Start', '04:03:12');
call pr_p2p_insert('transom','heel', 'C3_Stringplus','Success','04:04:16');

call pr_p2p_insert('transom','heel', 'C4_Math', 'Start', '04:05:12');
call pr_p2p_insert('transom','heel', 'C4_Math','Success','04:06:16');

call pr_p2p_insert('transom','heel', 'C5_Decimal', 'Start', '04:07:12');
call pr_p2p_insert('transom','heel', 'C5_Decimal','Success','04:08:16');

call pr_p2p_insert('transom','heel', 'C6_Matrix', 'Start', '04:09:12');
call pr_p2p_insert('transom','heel', 'C6_Matrix','Success','04:10:16');

call pr_p2p_insert('transom','heel', 'C7_SmartCalc_v1', 'Start', '04:11:12');
call pr_p2p_insert('transom','heel', 'C7_SmartCalc_v1','Success','04:12:16'); -- no verter

call pr_p2p_insert('transom','heel', 'C8_3DViewer_v1', 'Start', '04:13:12');
call pr_p2p_insert('transom','heel', 'C8_3DViewer_v1','Success','04:14:16'); -- no verter

-- today add
call pr_p2p_insert('transom','heel', 'C8_3DViewer_v1', 'Start', '04:25:12');
call pr_p2p_insert('transom','heel', 'C8_3DViewer_v1','Failure','04:26:16'); -- *

call pr_p2p_insert('transom','heel', 'C8_3DViewer_v1', 'Start', '04:30:12');
call pr_p2p_insert('transom','heel', 'C8_3DViewer_v1','Failure','04:31:16'); -- *

call pr_p2p_insert('transom','heel', 'C8_3DViewer_v1', 'Start', '04:43:12');
call pr_p2p_insert('transom','heel', 'C8_3DViewer_v1','Success','04:44:16'); -- no verter

-- jersey
call pr_p2p_insert('jersey','transom', 'C2_SimpleBashUtils', 'Start', '05:01:12');
call pr_p2p_insert('jersey','transom', 'C2_SimpleBashUtils','Success','05:02:16');

call pr_p2p_insert('jersey','transom', 'C3_Stringplus', 'Start', '05:03:12');
call pr_p2p_insert('jersey','transom', 'C3_Stringplus','Success','05:04:16');

call pr_p2p_insert('jersey','transom', 'C4_Math', 'Start', '05:05:12');
call pr_p2p_insert('jersey','transom', 'C4_Math','Success','05:06:16');

call pr_p2p_insert('jersey','transom', 'C5_Decimal', 'Start', '05:07:12');
call pr_p2p_insert('jersey','transom', 'C5_Decimal','Success','05:08:16');

call pr_p2p_insert('jersey','transom', 'C6_Matrix', 'Start', '05:09:12');
call pr_p2p_insert('jersey','transom', 'C6_Matrix','Success','05:10:16');

call pr_p2p_insert('jersey','transom', 'C7_SmartCalc_v1', 'Start', '05:11:12');
call pr_p2p_insert('jersey','transom', 'C7_SmartCalc_v1','Success','05:12:16'); -- no verter

call pr_p2p_insert('jersey','transom', 'C8_3DViewer_v1', 'Start', '05:13:12');
call pr_p2p_insert('jersey','transom', 'C8_3DViewer_v1','Success','05:14:16'); -- no verter

-- gainful
call pr_p2p_insert('gainful','jersey', 'C2_SimpleBashUtils', 'Start', '06:01:17');
call pr_p2p_insert('gainful','jersey', 'C2_SimpleBashUtils','Success','06:02:16');

call pr_p2p_insert('gainful','jersey', 'C3_Stringplus', 'Start', '06:03:12');
call pr_p2p_insert('gainful','jersey', 'C3_Stringplus','Success','06:04:16');

call pr_p2p_insert('gainful','jersey', 'C4_Math', 'Start', '06:05:12');
call pr_p2p_insert('gainful','jersey', 'C4_Math','Success','06:06:16'); -- verter fail

--------------------------------------------------------------------------------

call pr_p2p_insert('gainful','jersey', 'C5_Decimal', 'Start', '06:07:12');
call pr_p2p_insert('gainful','jersey', 'C5_Decimal','Success','06:08:16');

-- ornate
call pr_p2p_insert('ornate','gainful', 'C2_SimpleBashUtils', 'Start', '07:01:12');
call pr_p2p_insert('ornate','gainful', 'C2_SimpleBashUtils','Success','07:02:16'); 

-- CPP
call pr_p2p_insert('transom','heel', 'CPP1_s21_matrix', 'Start', '08:01:12');
call pr_p2p_insert('transom','heel', 'CPP1_s21_matrix','Success','08:02:16'); -- no verter

call pr_p2p_insert('heel','adequate', 'CPP3_SmartCalc_v2', 'Start', '08:03:12');
call pr_p2p_insert('heel','adequate', 'CPP3_SmartCalc_v2','Success','08:04:16'); -- no verter

call pr_p2p_insert('adequate','class', 'CPP7_MLP', 'Start', '08:05:12');
call pr_p2p_insert('adequate','class', 'CPP7_MLP','Failure','08:06:16'); -- *

call pr_p2p_insert('ultra','class', 'CPP7_MLP', 'Start', '08:07:12');
call pr_p2p_insert('ultra','class', 'CPP7_MLP','Success','08:08:16'); -- no verter

-- A
call pr_p2p_insert('class','duck', 'A1_Maze', 'Start', '09:01:12');
call pr_p2p_insert('class','duck', 'A1_Maze','Success','09:02:16'); -- no verter

-- Linux
call pr_p2p_insert('gainful','heel', 'D01_Linux', 'Start', '10:01:12');
call pr_p2p_insert('gainful','heel', 'D01_Linux','Success','10:02:16'); -- no verter

call pr_p2p_insert('adequate','duck', 'D02_Linux_Nexwork', 'Start', '10:03:12');
call pr_p2p_insert('adequate','duck', 'D02_Linux_Nexwork','Success','10:04:16'); -- no verter

-- SQL
call pr_p2p_insert('jersey','duck', 'SQL1_Pool', 'Start', '12:15:12');
call pr_p2p_insert('jersey','duck', 'SQL1_Pool','Failure','12:45:16'); -- *
call pr_p2p_insert('transom','duck', 'SQL1_Pool', 'Start', '12:15:12');
call pr_p2p_insert('transom','duck', 'SQL1_Pool','Failure','12:45:16'); -- *
call pr_p2p_insert('heel','duck', 'SQL1_Pool', 'Start', '12:15:12');
call pr_p2p_insert('heel','duck', 'SQL1_Pool','Failure','12:45:16'); -- *

call pr_p2p_insert('jersey','duck', 'SQL1_Pool', 'Start', '15:15:12');
call pr_p2p_insert('jersey','duck', 'SQL1_Pool','Success','15:45:16'); -- no verter
call pr_p2p_insert('transom','duck', 'SQL1_Pool', 'Start', '15:15:12');
call pr_p2p_insert('transom','duck', 'SQL1_Pool','Success','15:45:16'); -- no verter
call pr_p2p_insert('heel','duck', 'SQL1_Pool', 'Start', '15:15:12');
call pr_p2p_insert('heel','duck', 'SQL1_Pool','Success','15:45:16'); -- no verter

call pr_p2p_insert('adequate','duck', 'SQL1_Pool', 'Start', '16:15:12');
call pr_p2p_insert('adequate','duck', 'SQL1_Pool','Success','16:45:16'); -- no verter
call pr_p2p_insert('ultra','duck', 'SQL1_Pool', 'Start', '16:15:12');
call pr_p2p_insert('ultra','duck', 'SQL1_Pool','Success','16:45:16'); -- no verter
call pr_p2p_insert('class','duck', 'SQL1_Pool', 'Start', '16:15:12');
call pr_p2p_insert('class','duck', 'SQL1_Pool','Success','16:45:16'); -- no verter

-- CPP 
call pr_p2p_insert('adequate','duck', 'CPP7_MLP', 'Start', '17:15:12');
call pr_p2p_insert('adequate','duck', 'CPP7_MLP','Failure','17:45:16'); -- *
 


------------------------------------ VERTER ---------------------------------------------

call pr_verter_insert('class', 'C2_SimpleBashUtils', 'Start', '00:02:36');
call pr_verter_insert('class', 'C2_SimpleBashUtils', 'Success', '00:02:46');
call pr_verter_insert('class', 'C4_Math', 'Start', '00:06:36');
call pr_verter_insert('class', 'C4_Math', 'Success', '00:06:46');

call pr_verter_insert('class', 'C5_Decimal', 'Start', '00:08:36');
call pr_verter_insert('class', 'C5_Decimal', 'Success', '00:08:46');
call pr_verter_insert('class', 'C6_Matrix', 'Start', '00:10:36');
call pr_verter_insert('class', 'C6_Matrix', 'Success', '00:10:46');

call pr_verter_insert('ultra', 'C2_SimpleBashUtils', 'Start', '01:02:32');
call pr_verter_insert('ultra', 'C2_SimpleBashUtils', 'Success', '01:02:42');
call pr_verter_insert('ultra', 'C3_Stringplus', 'Start', '01:04:36');
call pr_verter_insert('ultra', 'C3_Stringplus', 'Failure', '01:04:46');
call pr_verter_insert('ultra', 'C4_Math', 'Start', '01:06:36');
call pr_verter_insert('ultra', 'C4_Math', 'Success', '01:06:56');

call pr_verter_insert('ultra', 'C5_Decimal', 'Start', '01:08:36');
call pr_verter_insert('ultra', 'C5_Decimal', 'Success', '01:08:46');
call pr_verter_insert('ultra', 'C6_Matrix', 'Start', '01:10:36');
call pr_verter_insert('ultra', 'C6_Matrix', 'Failure', '01:10:46');

call pr_verter_insert('adequate', 'C3_Stringplus', 'Start', '02:04:36');
call pr_verter_insert('adequate', 'C3_Stringplus', 'Success', '02:04:46');
call pr_verter_insert('adequate', 'C4_Math', 'Start','02:06:36');
call pr_verter_insert('adequate', 'C4_Math', 'Success','02:06:46');
call pr_verter_insert('adequate', 'C5_Decimal', 'Start', '02:08:36');
call pr_verter_insert('adequate', 'C5_Decimal', 'Success', '02:08:46');
call pr_verter_insert('adequate', 'C6_Matrix', 'Start', '02:10:36');
call pr_verter_insert('adequate', 'C6_Matrix', 'Failure', '02:10:56');

call pr_verter_insert('adequate', 'C2_SimpleBashUtils','Start','02:41:36');
call pr_verter_insert('adequate', 'C2_SimpleBashUtils','Success','02:41:46');
call pr_verter_insert('adequate', 'C3_Stringplus','Start','02:58:32');
call pr_verter_insert('adequate', 'C3_Stringplus','Success','02:58:42');

call pr_verter_insert('heel', 'C2_SimpleBashUtils','Start','03:02:36');
call pr_verter_insert('heel', 'C2_SimpleBashUtils','Success','03:02:46');
call pr_verter_insert('heel', 'C3_Stringplus','Start','03:04:36');
call pr_verter_insert('heel', 'C3_Stringplus','Success','03:04:56');
call pr_verter_insert('heel', 'C4_Math','Start','03:06:36');
call pr_verter_insert('heel', 'C4_Math','Success','03:06:46');
call pr_verter_insert('heel', 'C5_Decimal','Start','03:08:36');
call pr_verter_insert('heel', 'C5_Decimal','Failure','03:08:46');
call pr_verter_insert('heel', 'C6_Matrix','Start','03:10:36');
call pr_verter_insert('heel', 'C6_Matrix','Success','03:10:56');

call pr_verter_insert('transom', 'C2_SimpleBashUtils','Start','04:02:36');
call pr_verter_insert('transom', 'C2_SimpleBashUtils','Success','04:02:46');
call pr_verter_insert('transom', 'C3_Stringplus','Start','04:04:36');
call pr_verter_insert('transom', 'C3_Stringplus','Success','04:04:56');
call pr_verter_insert('transom', 'C4_Math','Start','04:06:36');
call pr_verter_insert('transom', 'C4_Math','Success','04:06:46');
call pr_verter_insert('transom', 'C5_Decimal','Start','04:08:36');
call pr_verter_insert('transom', 'C5_Decimal','Success','04:08:56');
call pr_verter_insert('transom', 'C6_Matrix','Start','04:10:36');
call pr_verter_insert('transom', 'C6_Matrix','Success','04:10:46');

call pr_verter_insert('jersey', 'C2_SimpleBashUtils','Start','05:02:36');
call pr_verter_insert('jersey', 'C2_SimpleBashUtils','Success','05:02:46');
call pr_verter_insert('jersey', 'C3_Stringplus','Start','05:04:36');
call pr_verter_insert('jersey', 'C3_Stringplus','Success','05:04:56');
call pr_verter_insert('jersey', 'C4_Math','Start','05:06:36');
call pr_verter_insert('jersey', 'C4_Math','Success','05:06:56');
call pr_verter_insert('jersey', 'C5_Decimal','Start','05:08:36');
call pr_verter_insert('jersey', 'C5_Decimal','Success','05:08:46');
call pr_verter_insert('jersey', 'C6_Matrix','Start','05:10:36');
call pr_verter_insert('jersey', 'C6_Matrix','Success','05:10:46');

call pr_verter_insert('gainful', 'C2_SimpleBashUtils','Start','06:02:36');
call pr_verter_insert('gainful', 'C2_SimpleBashUtils','Success','06:02:46');
call pr_verter_insert('gainful', 'C3_Stringplus','Start','06:04:26');
call pr_verter_insert('gainful', 'C3_Stringplus','Success','06:04:36');
call pr_verter_insert('gainful', 'C4_Math','Start','06:06:36');
call pr_verter_insert('gainful', 'C4_Math','Failure','06:06:46');

call pr_verter_insert('gainful', 'C5_Decimal','Start','06:08:36');
call pr_verter_insert('gainful', 'C5_Decimal','Success','06:08:56');

call pr_verter_insert('ornate', 'C2_SimpleBashUtils','Start','07:02:36');
call pr_verter_insert('ornate', 'C2_SimpleBashUtils','Success','07:02:46');

------------------------------------ XP ---------------------------------------------

-- 3ий аргумент-время старта успешкой п2п проверки, а XP указывается в процентах от максимального!

call pr_xp_insert('class','C2_SimpleBashUtils', '00:01:12', 95);
call pr_xp_insert('class', 'C4_Math', '00:05:16', 100);
call pr_xp_insert('class', 'C5_Decimal', '00:07:12', 83);
call pr_xp_insert('class', 'C6_Matrix', '00:09:12', 100);
call pr_xp_insert('class', 'C7_SmartCalc_v1', '00:11:12', 100);
call pr_xp_insert('class', 'C8_3DViewer_v1', '00:13:12', 90);
call pr_xp_insert('class', 'CPP1_s21_matrix', '00:15:12', 75);
call pr_xp_insert('class', 'CPP2_s21_containers', '00:17:12', 98);
call pr_xp_insert('class', 'CPP3_SmartCalc_v2', '00:19:12', 100);
call pr_xp_insert('class', 'CPP4_3DViewer_v2', '00:30:12', 100);

call pr_xp_insert('ultra', 'C2_SimpleBashUtils', '01:01:12', 67);
call pr_xp_insert('ultra', 'C4_Math', '01:05:12', 80);
call pr_xp_insert('ultra', 'C5_Decimal', '01:07:12', 100);
call pr_xp_insert('ultra', 'C7_SmartCalc_v1', '01:11:12', 100);
call pr_xp_insert('ultra', 'C8_3DViewer_v1', '01:13:12', 100);
call pr_xp_insert('ultra', 'CPP1_s21_matrix', '01:15:12', 95);
call pr_xp_insert('ultra', 'CPP2_s21_containers', '01:17:12', 90);
call pr_xp_insert('ultra', 'CPP3_SmartCalc_v2', '01:19:12', 100);
call pr_xp_insert('ultra', 'CPP4_3DViewer_v2', '01:21:12', 78);

call pr_xp_insert('adequate', 'C3_Stringplus', '02:03:12', 87);
call pr_xp_insert('adequate', 'C4_Math', '02:05:12', 100);
call pr_xp_insert('adequate', 'C5_Decimal','02:07:12', 80);
call pr_xp_insert('adequate', 'C7_SmartCalc_v1', '02:11:12', 67);

call pr_xp_insert('adequate', 'C8_3DViewer_v1', '02:13:12', 100);
call pr_xp_insert('adequate', 'CPP1_s21_matrix', '02:15:12', 95);
call pr_xp_insert('adequate', 'CPP2_s21_containers', '02:17:12', 90);
call pr_xp_insert('adequate', 'CPP3_SmartCalc_v2', '02:19:12', 100);
call pr_xp_insert('adequate', 'CPP4_3DViewer_v2', '02:21:12', 100);
call pr_xp_insert('adequate', 'D01_Linux', '02:23:12', 100);
call pr_xp_insert('adequate', 'C2_SimpleBashUtils', '02:38:12', 90);
call pr_xp_insert('adequate', 'C3_Stringplus', '02:44:12', 90);

call pr_xp_insert('heel', 'C2_SimpleBashUtils', '03:01:12', 65);
call pr_xp_insert('heel', 'C3_Stringplus', '03:03:12', 100);
call pr_xp_insert('heel', 'C4_Math', '03:05:12', 100);
call pr_xp_insert('heel', 'C6_Matrix', '03:09:12', 90);
call pr_xp_insert('heel', 'C7_SmartCalc_v1', '03:11:12', 90);
call pr_xp_insert('heel', 'C8_3DViewer_v1', '03:13:12', 100);
call pr_xp_insert('heel', 'CPP1_s21_matrix', '03:15:12', 100);
call pr_xp_insert('heel', 'CPP2_s21_containers', '03:17:12', 100);
call pr_xp_insert('heel', 'D01_Linux', '03:19:12', 95);

call pr_xp_insert('transom', 'C2_SimpleBashUtils', '04:01:12', 95);
call pr_xp_insert('transom', 'C3_Stringplus', '04:03:12', 100);
call pr_xp_insert('transom', 'C4_Math', '04:05:12', 100);
call pr_xp_insert('transom', 'C5_Decimal', '04:07:12', 100);
call pr_xp_insert('transom', 'C6_Matrix', '04:09:12', 90);
call pr_xp_insert('transom', 'C7_SmartCalc_v1', '04:11:12', 100);
call pr_xp_insert('transom', 'C8_3DViewer_v1', '04:13:12', 65);
call pr_xp_insert('transom', 'C8_3DViewer_v1', '04:43:12', 100);

call pr_xp_insert('jersey', 'C2_SimpleBashUtils', '05:01:12', 100);
call pr_xp_insert('jersey', 'C3_Stringplus', '05:03:12', 100);
call pr_xp_insert('jersey', 'C4_Math', '05:05:12', 90);
call pr_xp_insert('jersey', 'C5_Decimal', '05:07:12', 100);
call pr_xp_insert('jersey', 'C6_Matrix', '05:09:12', 100);
call pr_xp_insert('jersey', 'C7_SmartCalc_v1', '05:11:12', 90);
call pr_xp_insert('jersey', 'C8_3DViewer_v1', '05:13:12', 100);

call pr_xp_insert('gainful', 'C2_SimpleBashUtils', '06:01:17', 100);
call pr_xp_insert('gainful', 'C3_Stringplus', '06:03:12', 100);

call pr_xp_insert('gainful', 'C5_Decimal', '06:07:12', 100);

call pr_xp_insert('ornate', 'C2_SimpleBashUtils', '07:01:12', 100);

call pr_xp_insert('transom', 'CPP1_s21_matrix', '08:01:12', 90);
call pr_xp_insert('heel', 'CPP3_SmartCalc_v2', '08:03:12', 100);
call pr_xp_insert('ultra', 'CPP7_MLP', '08:07:12', 100);
call pr_xp_insert('class', 'A1_Maze', '09:01:12', 100);

call pr_xp_insert('gainful', 'D01_Linux', '10:01:12', 100);
call pr_xp_insert('adequate', 'D02_Linux_Nexwork', '10:03:12', 100);

call pr_xp_insert('jersey', 'SQL1_Pool', '15:15:12', 100);
call pr_xp_insert('transom', 'SQL1_Pool', '15:15:12', 100);
call pr_xp_insert('heel', 'SQL1_Pool', '15:15:12', 100);

call pr_xp_insert('adequate', 'SQL1_Pool', '16:15:12', 90);
call pr_xp_insert('ultra', 'SQL1_Pool', '16:15:12', 90);
call pr_xp_insert('class', 'SQL1_Pool', '16:15:12', 90);


------------------------------UPDATE checks----------------------------- 
UPDATE checks SET "date" = '2022-11-10' WHERE id <= 15;
UPDATE checks SET "date" = '2022-11-15' WHERE id > 15 AND id <= 30;
UPDATE checks SET "date" = '2022-11-21' WHERE id > 30 AND id <= 50;
UPDATE checks SET "date" = '2022-11-30' WHERE id > 50 AND id <= 72;
UPDATE checks SET "date" = '2022-12-02' WHERE id > 72;
