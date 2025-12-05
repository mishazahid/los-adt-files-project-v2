# PowerShell script to set up virtual environment for the project

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "Setting up Virtual Environment" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""

# Check if venv already exists
if (Test-Path "venv") {
    Write-Host "⚠️  Virtual environment already exists!" -ForegroundColor Yellow
    $response = Read-Host "Do you want to recreate it? (y/n)"
    if ($response -eq "y") {
        Write-Host "Removing existing virtual environment..." -ForegroundColor Yellow
        Remove-Item -Recurse -Force venv
    } else {
        Write-Host "Using existing virtual environment." -ForegroundColor Green
        Write-Host ""
        Write-Host "To activate it, run:" -ForegroundColor Cyan
        Write-Host "  .\venv\Scripts\Activate.ps1" -ForegroundColor White
        exit
    }
}

Write-Host "Creating virtual environment..." -ForegroundColor Green
python -m venv venv

Write-Host ""
Write-Host "✅ Virtual environment created!" -ForegroundColor Green
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Cyan
Write-Host "1. Activate the virtual environment:" -ForegroundColor White
Write-Host "   .\venv\Scripts\Activate.ps1" -ForegroundColor Yellow
Write-Host ""
Write-Host "2. Install dependencies:" -ForegroundColor White
Write-Host "   pip install -r requirements.txt" -ForegroundColor Yellow
Write-Host ""
Write-Host "3. Start the server:" -ForegroundColor White
Write-Host "   python start_server.py" -ForegroundColor Yellow
Write-Host ""

