// Frontend JavaScript for Facility Report Generator

const API_BASE = '/api';

// DOM elements
const adtInput = document.getElementById('adtFiles');
const losInput = document.getElementById('losFiles');
const visitInput = document.getElementById('visitFiles');
const uploadBtn = document.getElementById('uploadBtn');
const processingSection = document.getElementById('processingSection');
const resultsSection = document.getElementById('resultsSection');
const errorSection = document.getElementById('errorSection');

// File lists
const adtFileList = document.getElementById('adtFileList');
const losFileList = document.getElementById('losFileList');
const visitFileList = document.getElementById('visitFileList');

// Status elements
const jobIdEl = document.getElementById('jobId');
const jobStatusEl = document.getElementById('jobStatus');
const progressBar = document.getElementById('progressBar');
const progressText = document.getElementById('progressText');
const logsContainer = document.getElementById('logsContainer');
const downloadLinks = document.getElementById('downloadLinks');
const driveLinks = document.getElementById('driveLinks');
const errorMessage = document.getElementById('errorMessage');

// File arrays
let adtFiles = [];
let losFiles = [];
let visitFiles = [];

// Current job ID
let currentJobId = null;
let statusCheckInterval = null;

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    setupFileInputs();
    uploadBtn.addEventListener('click', handleUpload);
    
    const retryBtn = document.getElementById('retryBtn');
    if (retryBtn) {
        retryBtn.addEventListener('click', () => {
            resetUI();
        });
    }
});

function setupFileInputs() {
    adtInput.addEventListener('change', (e) => {
        adtFiles = Array.from(e.target.files);
        updateFileList(adtFileList, adtFiles, 'adt');
        checkUploadButton();
    });

    losInput.addEventListener('change', (e) => {
        losFiles = Array.from(e.target.files);
        updateFileList(losFileList, losFiles, 'los');
        checkUploadButton();
    });

    visitInput.addEventListener('change', (e) => {
        visitFiles = Array.from(e.target.files);
        updateFileList(visitFileList, visitFiles, 'visit');
        checkUploadButton();
    });
}

function updateFileList(container, files, type) {
    container.innerHTML = '';
    files.forEach((file, index) => {
        const fileItem = document.createElement('div');
        fileItem.className = 'file-item';
        fileItem.innerHTML = `
            <span>${file.name}</span>
            <span class="file-remove" onclick="removeFile('${type}', ${index})">×</span>
        `;
        container.appendChild(fileItem);
    });
}

function removeFile(type, index) {
    if (type === 'adt') {
        adtFiles.splice(index, 1);
        updateFileList(adtFileList, adtFiles, 'adt');
        updateFileInput(adtInput, adtFiles);
    } else if (type === 'los') {
        losFiles.splice(index, 1);
        updateFileList(losFileList, losFiles, 'los');
        updateFileInput(losInput, losFiles);
    } else if (type === 'visit') {
        visitFiles.splice(index, 1);
        updateFileList(visitFileList, visitFiles, 'visit');
        updateFileInput(visitInput, visitFiles);
    }
    checkUploadButton();
}

function updateFileInput(input, files) {
    const dt = new DataTransfer();
    files.forEach(file => dt.items.add(file));
    input.files = dt.files;
}

function checkUploadButton() {
    const hasFiles = adtFiles.length > 0 || losFiles.length > 0 || visitFiles.length > 0;
    uploadBtn.disabled = !hasFiles;
}

async function handleUpload() {
    if (adtFiles.length === 0 && losFiles.length === 0 && visitFiles.length === 0) {
        alert('Please select at least one file to upload.');
        return;
    }

    // Prepare form data
    const formData = new FormData();
    
    adtFiles.forEach(file => {
        formData.append('adt_files', file);
    });
    
    losFiles.forEach(file => {
        formData.append('los_files', file);
    });
    
    visitFiles.forEach(file => {
        formData.append('visit_files', file);
    });

    // Show processing section
    processingSection.style.display = 'block';
    resultsSection.style.display = 'none';
    errorSection.style.display = 'none';
    
    uploadBtn.disabled = true;
    uploadBtn.querySelector('.btn-text').textContent = 'Uploading...';
    
    try {
        const response = await fetch(`${API_BASE}/upload/files`, {
            method: 'POST',
            body: formData
        });

        const data = await response.json();

        if (!response.ok) {
            throw new Error(data.detail || 'Upload failed');
        }

        currentJobId = data.job_id;
        jobIdEl.textContent = currentJobId;
        jobStatusEl.textContent = 'Uploaded';
        
        // Start polling for status
        startStatusPolling(currentJobId);
        
        uploadBtn.querySelector('.btn-text').textContent = 'Upload & Process Files';
        
    } catch (error) {
        showError(error.message);
        uploadBtn.disabled = false;
        uploadBtn.querySelector('.btn-text').textContent = 'Upload & Process Files';
    }
}

function startStatusPolling(jobId) {
    // Clear any existing interval
    if (statusCheckInterval) {
        clearInterval(statusCheckInterval);
    }
    
    // Poll immediately
    checkJobStatus(jobId);
    
    // Then poll every 2 seconds
    statusCheckInterval = setInterval(() => {
        checkJobStatus(jobId);
    }, 2000);
    
    // Also start log streaming
    startLogStreaming(jobId);
}

async function checkJobStatus(jobId) {
    try {
        const response = await fetch(`${API_BASE}/status/${jobId}`);
        const status = await response.json();

        if (!response.ok) {
            throw new Error(status.detail || 'Failed to get status');
        }

        // Update status display
        jobStatusEl.textContent = status.status || 'Unknown';
        progressBar.style.width = `${status.progress || 0}%`;
        progressText.textContent = `${status.progress || 0}%`;

        // Check if job is complete
        if (status.status === 'completed') {
            clearInterval(statusCheckInterval);
            showResults(jobId, status);
        } else if (status.status === 'error') {
            clearInterval(statusCheckInterval);
            // Get error message from multiple sources
            let errorMsg = status.message || '';
            if (status.errors && status.errors.length > 0) {
                errorMsg = status.errors.join('; ');
            }
            if (!errorMsg) {
                errorMsg = 'Processing failed. Check server logs for details.';
            }
            showError(errorMsg);
        }

    } catch (error) {
        console.error('Error checking status:', error);
        // Don't stop polling on individual errors
    }
}

async function startLogStreaming(jobId) {
    try {
        const response = await fetch(`${API_BASE}/status/${jobId}/logs/tail?lines=100`);
        const data = await response.json();

        if (data.logs) {
            logsContainer.innerHTML = '';
            const lines = data.logs.split('\n');
            lines.forEach(line => {
                if (line.trim()) {
                    const logEntry = document.createElement('div');
                    logEntry.className = 'log-entry';
                    
                    // Determine log level
                    if (line.includes('ERROR') || line.includes('Error')) {
                        logEntry.className += ' error';
                    } else if (line.includes('✓') || line.includes('Success')) {
                        logEntry.className += ' success';
                    } else {
                        logEntry.className += ' info';
                    }
                    
                    logEntry.textContent = line;
                    logsContainer.appendChild(logEntry);
                }
            });
            
            // Scroll to bottom
            logsContainer.scrollTop = logsContainer.scrollHeight;
        }

        // Continue polling logs while job is active
        if (statusCheckInterval) {
            setTimeout(() => startLogStreaming(jobId), 3000);
        }
    } catch (error) {
        console.error('Error fetching logs:', error);
    }
}

async function showResults(jobId, status) {
    processingSection.style.display = 'none';
    resultsSection.style.display = 'block';
    errorSection.style.display = 'none';

    // Fetch download links
    try {
        const response = await fetch(`${API_BASE}/download/${jobId}`);
        const downloadData = await response.json();

        if (downloadData.files) {
            downloadLinks.innerHTML = '';
            downloadData.files.forEach(file => {
                const link = document.createElement('a');
                link.href = file.url;
                link.className = 'download-link';
                link.textContent = `📥 ${file.name}`;
                link.download = file.name;
                downloadLinks.appendChild(link);
            });
        }

        // Show Google service links from status
        if (status.outputs && status.outputs.links) {
            driveLinks.innerHTML = '';
            const linksData = status.outputs.links;
            
            if (linksData.google_sheets) {
                const link = document.createElement('a');
                link.href = linksData.google_sheets;
                link.target = '_blank';
                link.className = 'drive-link';
                link.textContent = '📊 Google Sheets';
                driveLinks.appendChild(link);
            }
            
            if (linksData.generated_pdf) {
                // Check if it's a URL or just a message
                if (linksData.generated_pdf.startsWith('http')) {
                    const linkContainer = document.createElement('div');
                    linkContainer.className = 'drive-link-container';
                    linkContainer.style.marginBottom = '10px';
                    
                    const link = document.createElement('a');
                    link.href = linksData.generated_pdf;
                    link.target = '_blank';
                    link.className = 'drive-link';
                    link.textContent = '📄 View PDF on Google Drive';
                    link.style.display = 'block';
                    link.style.marginBottom = '5px';
                    
                    // Add the actual URL as a smaller text below
                    const urlText = document.createElement('span');
                    urlText.className = 'drive-url';
                    urlText.style.fontSize = '0.85em';
                    urlText.style.color = '#666';
                    urlText.style.wordBreak = 'break-all';
                    urlText.textContent = linksData.generated_pdf;
                    
                    linkContainer.appendChild(link);
                    linkContainer.appendChild(urlText);
                    driveLinks.appendChild(linkContainer);
                } else {
                    // If it's just a message, show it as text with a note
                    const text = document.createElement('span');
                    text.className = 'drive-link';
                    text.style.display = 'block';
                    text.style.marginTop = '10px';
                    text.textContent = `📄 ${linksData.generated_pdf}`;
                    driveLinks.appendChild(text);
                }
            }
            
            if (linksData.google_slides) {
                const link = document.createElement('a');
                link.href = linksData.google_slides;
                link.target = '_blank';
                link.className = 'drive-link';
                link.textContent = '📑 Google Slides';
                driveLinks.appendChild(link);
            }
        }

    } catch (error) {
        console.error('Error fetching download links:', error);
    }
}

function showError(message) {
    processingSection.style.display = 'none';
    resultsSection.style.display = 'none';
    errorSection.style.display = 'block';
    errorMessage.textContent = message;
}

function resetUI() {
    adtFiles = [];
    losFiles = [];
    visitFiles = [];
    
    adtInput.value = '';
    losInput.value = '';
    visitInput.value = '';
    
    adtFileList.innerHTML = '';
    losFileList.innerHTML = '';
    visitFileList.innerHTML = '';
    
    processingSection.style.display = 'none';
    resultsSection.style.display = 'none';
    errorSection.style.display = 'none';
    
    currentJobId = null;
    if (statusCheckInterval) {
        clearInterval(statusCheckInterval);
        statusCheckInterval = null;
    }
    
    checkUploadButton();
}

