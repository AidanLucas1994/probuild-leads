console.log("Application loaded");

document.addEventListener('DOMContentLoaded', function() {
    // View switching functionality
    const viewBtns = document.querySelectorAll('.view-btn');
    const views = document.querySelectorAll('.view-container > div');
    
    viewBtns.forEach(btn => {
        btn.addEventListener('click', function() {
            const viewType = this.dataset.view;
            
            viewBtns.forEach(b => b.classList.remove('active'));
            views.forEach(v => v.classList.remove('active'));
            
            this.classList.add('active');
            document.querySelector(`.${viewType}-view`).classList.add('active');
        });
    });

    // Modal functionality
    const modal = document.getElementById('leadDetailsModal');
    const detailsBtns = document.querySelectorAll('.details-btn');
    const closeBtn = document.querySelector('.close-modal');

    detailsBtns.forEach(btn => {
        btn.addEventListener('click', async function() {
            const leadId = this.dataset.leadId;
            try {
                const response = await fetch(`/api/leads/${leadId}`);
                if (!response.ok) throw new Error('Failed to fetch lead details');
                
                const lead = await response.json();
                
                // Update modal content with lead details
                updateModalContent(lead);
                
                // Show modal
                modal.style.display = 'block';
                document.body.style.overflow = 'hidden'; // Prevent background scrolling
            } catch (error) {
                console.error('Error fetching lead details:', error);
                // Show error message to user
                showNotification('Error loading lead details. Please try again.', 'error');
            }
        });
    });

    // Close modal functionality
    function closeModal() {
        modal.style.display = 'none';
        document.body.style.overflow = ''; // Restore scrolling
    }

    closeBtn.addEventListener('click', closeModal);
    
    window.addEventListener('click', function(event) {
        if (event.target === modal) {
            closeModal();
        }
    });

    // Escape key to close modal
    document.addEventListener('keydown', function(event) {
        if (event.key === 'Escape' && modal.style.display === 'block') {
            closeModal();
        }
    });

    // Function to update modal content with lead details
    function updateModalContent(lead) {
        const modalContent = modal.querySelector('.modal-content');
        
        // Update each field in the modal
        const fields = {
            'permit-number': lead.permit_number,
            'permit-status': lead.permit_status,
            'permit-description': lead.permit_description,
            'project-type': lead.project_type,
            'work-type': lead.work_type,
            'sub-work-type': lead.sub_work_type,
            'location': lead.location,
            'legal-description': lead.legal_description,
            'construction-value': formatCurrency(lead.construction_value),
            'total-units': lead.total_units,
            'units-created': lead.units_created,
            'submission-date': formatDate(lead.submission_date),
            'issue-date': formatDate(lead.issue_date),
            'expiry-date': formatDate(lead.expiry_date),
            'owner': lead.owner,
            'applicant': lead.applicant,
            'contractor': lead.contractor,
            'contractor-contact': lead.contractor_contact
        };
        
        // Update each field in the modal
        for (const [key, value] of Object.entries(fields)) {
            const element = modalContent.querySelector(`.${key}`);
            if (element) {
                element.textContent = value || 'N/A';
            }
        }
        
        // Update status badge
        const statusBadge = modalContent.querySelector('.permit-status');
        if (statusBadge) {
            statusBadge.className = `status-badge status-${lead.permit_status.toLowerCase()}`;
        }
    }

    // Helper function to format currency
    function formatCurrency(value) {
        if (!value) return 'N/A';
        return new Intl.NumberFormat('en-CA', {
            style: 'currency',
            currency: 'CAD'
        }).format(value);
    }

    // Helper function to format dates
    function formatDate(dateString) {
        if (!dateString) return 'N/A';
        return new Date(dateString).toLocaleDateString('en-CA', {
            year: 'numeric',
            month: 'long',
            day: 'numeric'
        });
    }

    // Function to show notifications
    function showNotification(message, type = 'success') {
        const notification = document.createElement('div');
        notification.className = `flash-message ${type}`;
        notification.textContent = message;
        
        const container = document.querySelector('.flash-messages');
        if (!container) {
            const newContainer = document.createElement('div');
            newContainer.className = 'flash-messages';
            document.body.appendChild(newContainer);
            newContainer.appendChild(notification);
        } else {
            container.appendChild(notification);
        }
        
        // Auto-hide after 5 seconds
        setTimeout(() => {
            notification.style.opacity = '0';
            setTimeout(() => notification.remove(), 300);
        }, 5000);
    }
});
