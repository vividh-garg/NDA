# NDA

This includes two files nda_version_8_0.py & nda_functions.py
# nda_version_8_0.py
 File to decode data from .nda file to usable form.
# nda_functions.py
File that includes lot of functions that allows users to get data from the decoded data. Below are the function names to call and the arguments to pass.
  
### records
Args: (nda file path)
- Returns a Dataframe record-wise for the nda file.
- Add second arguement as True if you want the coloumns renamed (data units converted) to same as that produced in the NEWARE excel file
### cycle
Args: (records dataframe or nda file path)<br>
When passed an nda file or the records data, it returns Cycle-wise data identical to the cycle sheet in the excel file of the test.
### step
Args: (records dataframe or nda file path)<br>
When passed an nda file or the records data, it returns Step-wise data identical to the cycle sheet in the excel file of the test.

### get_process_name
Args: (nda file path)<br>
returns Recipe Name for passed NDA file
### get_barcode
Args: (nda file path)<br
returns Barcode for passed NDA file
### get_start_time
Args: (nda file path)<br>
returns Start Time for passed NDA file


   
                   
             
