@echo off
cd /d "C:\Users\MSI\Desktop\workspace\python_automation_script"
call "c:\Users\MSI\Desktop\workspace\python_automation_script\.venv\Scripts\activate.bat"
python "c:\Users\MSI\Desktop\workspace\python_automation_script\mention_created_based_on_social_id.py" %*
call deactivate
