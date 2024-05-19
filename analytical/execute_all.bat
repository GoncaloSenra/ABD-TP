
set "OUTPUT_DIRECTORY=.\results\before_index_2\"
rem set "OUTPUT_DIRECTORY=.\results\questions_owneruserid_idx\"

echo "Q1"
call .\execute.bat Q1 %OUTPUT_DIRECTORY%

echo "Q1 V1"
call .\execute.bat Q1_V1 %OUTPUT_DIRECTORY%

echo "Q1 V2"
call .\execute.bat Q1_V2 %OUTPUT_DIRECTORY%
