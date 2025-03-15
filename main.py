from flask import Flask, render_template, request, redirect, url_for, flash, send_file, jsonify
from datetime import datetime, timedelta
from collections import Counter
import random
import csv
import io
import os
import sys
import logging
from models import db, Lead

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Get the absolute path of the current directory
BASE_DIR = os.path.abspath(os.path.dirname(__file__))

# Ensure template and static directories exist
template_dir = os.path.join(BASE_DIR, 'templates')
static_dir = os.path.join(BASE_DIR, 'static')

try:
    os.makedirs(template_dir, exist_ok=True)
    os.makedirs(static_dir, exist_ok=True)
    logger.info(f"Template directory: {template_dir}")
    logger.info(f"Static directory: {static_dir}")
except Exception as e:
    logger.error(f"Error creating directories: {e}")
    # Don't exit, just log the error
    logger.warning("Continuing despite directory creation error")

# Initialize Flask app with explicit template and static folder paths
app = Flask(__name__,
            template_folder=template_dir,
            static_folder=static_dir)

# Configure SQLAlchemy
database_url = os.environ.get('DATABASE_URL')
if database_url and database_url.startswith('postgres://'):
    # Render uses postgres:// but SQLAlchemy requires postgresql://
    database_url = database_url.replace('postgres://', 'postgresql://', 1)
    logger.info("Using PostgreSQL database")
else:
    # Default to SQLite for local development
    database_url = f'sqlite:///{os.path.join(BASE_DIR, "leads.db")}'
    logger.info("Using SQLite database")

app.config['SQLALCHEMY_DATABASE_URI'] = database_url
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Use environment variable for secret key with a fallback
app.secret_key = os.environ.get('SECRET_KEY', 'dev-key-please-change-in-production')
# Disable debug mode in production
app.debug = os.environ.get('FLASK_ENV') == 'development'

# Initialize the database
db.init_app(app)

# Create database tables
try:
    with app.app_context():
        db.create_all()
        logger.info("Database tables created successfully")
except Exception as e:
    logger.error(f"Error creating database tables: {e}")

def generate_sample_data(num_samples=15):
    """Generate simulated building permit data."""
    project_types = [
        'Residential Building (House)',
        'Commercial Building',
        'Industrial Building',
        'Residential Alteration',
        'Non-Residential Alteration'
    ]
    
    work_types = [
        'New Construction',
        'Addition to Building',
        'Interior Alteration',
        'Exterior Alteration',
        'Renovation'
    ]
    
    sub_work_types = [
        'Single Detached Dwelling',
        'Commercial',
        'Industrial',
        'Multi-Unit Residential',
        'Office Building'
    ]
    
    locations = [
        'Toronto, ON', 'Vancouver, BC', 'Montreal, QC', 'Calgary, AB',
        'Ottawa, ON', 'Edmonton, AB', 'Winnipeg, MB', 'Quebec City, QC',
        'Hamilton, ON', 'Victoria, BC', 'Halifax, NS', 'Saskatoon, SK'
    ]
    
    statuses = ['Pending', 'Approved', 'In Review', 'Closed', 'Expired']
    
    # Generate random dates within the last 30 days
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    sample_data = []
    for i in range(num_samples):
        submission_date = start_date + timedelta(
            seconds=random.randint(0, int((end_date - start_date).total_seconds()))
        )
        
        # Generate random coordinates in Canada
        lat = random.uniform(43.0, 50.0)
        lon = random.uniform(-123.0, -79.0)
        
        # Generate random construction value
        construction_value = random.randint(10000, 2000000)
        
        lead = Lead(
            permit_number=f'BP{datetime.now().year}{i+1:04d}',
            project_type=random.choice(project_types),
            permit_status=random.choice(statuses),
            permit_description=f'New {random.choice(sub_work_types).lower()} project',
            submission_date=submission_date,
            issue_date=submission_date + timedelta(days=random.randint(5, 15)) if random.random() > 0.3 else None,
            expiry_date=submission_date + timedelta(days=365),
            location=random.choice(locations),
            work_type=random.choice(work_types),
            sub_work_type=random.choice(sub_work_types),
            construction_value=construction_value,
            total_units=random.randint(1, 10),
            units_created=random.randint(0, 5),
            legal_description=f'PLAN {random.randint(1000, 9999)} LOT {random.randint(1, 999)}',
            latitude=lat,
            longitude=lon,
            owner=f'Owner {i+1}',
            applicant=f'Applicant {i+1}',
            contractor=f'Contractor {i+1}',
            contractor_contact=f'contact{i+1}@example.com'
        )
        sample_data.append(lead)
    
    return sample_data

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        project_type = request.form.get('project_type')
        location = request.form.get('location')
        value_range = request.form.get('value_range')

        if not all([project_type, location, value_range]):
            flash('All fields are required!', 'error')
            return redirect(url_for('index'))

        # Create a new lead
        lead = Lead(
            project_type=project_type,
            location=location,
            value_range=f"CAD {value_range}",
            submission_date=datetime.now()
        )
        db.session.add(lead)
        db.session.commit()

        flash('Lead submitted successfully!', 'success')
        return redirect(url_for('dashboard'))

    return render_template('index.html')

@app.route('/dashboard')
def dashboard():
    # Get filter parameters
    project_type_filter = request.args.get('project_type')
    location_filter = request.args.get('location')
    status_filter = request.args.get('status')
    work_type_filter = request.args.get('work_type')
    min_value = request.args.get('min_value', type=float)
    max_value = request.args.get('max_value', type=float)
    date_from = request.args.get('date_from')
    date_to = request.args.get('date_to')

    # Start with base query
    query = Lead.query

    # Apply filters
    if project_type_filter:
        query = query.filter_by(project_type=project_type_filter)
    if location_filter:
        query = query.filter_by(location=location_filter)
    if status_filter:
        query = query.filter_by(permit_status=status_filter)
    if work_type_filter:
        query = query.filter_by(work_type=work_type_filter)
    if min_value is not None:
        query = query.filter(Lead.construction_value >= min_value)
    if max_value is not None:
        query = query.filter(Lead.construction_value <= max_value)
    if date_from:
        query = query.filter(Lead.submission_date >= datetime.strptime(date_from, '%Y-%m-%d'))
    if date_to:
        query = query.filter(Lead.submission_date <= datetime.strptime(date_to, '%Y-%m-%d'))

    # Execute query
    filtered_leads = query.order_by(Lead.submission_date.desc()).all()
    
    # If no leads exist, generate and add sample data
    if not filtered_leads and not any([project_type_filter, location_filter, status_filter, 
                                     work_type_filter, min_value, max_value, date_from, date_to]):
        sample_leads = generate_sample_data()
        for lead in sample_leads:
            db.session.add(lead)
        db.session.commit()
        filtered_leads = Lead.query.order_by(Lead.submission_date.desc()).all()

    # Calculate statistics
    total_leads = len(filtered_leads)
    total_value = sum(lead.construction_value for lead in filtered_leads)
    avg_value = total_value / total_leads if total_leads > 0 else 0
    
    # Count by various categories
    project_types = Counter(lead.project_type for lead in filtered_leads)
    work_types = Counter(lead.work_type for lead in filtered_leads)
    statuses = Counter(lead.permit_status for lead in filtered_leads)
    
    # Get unique values for filter dropdowns from all leads
    all_leads = Lead.query.all()
    available_filters = {
        'project_types': sorted(set(lead.project_type for lead in all_leads)),
        'locations': sorted(set(lead.location for lead in all_leads)),
        'statuses': sorted(set(lead.permit_status for lead in all_leads)),
        'work_types': sorted(set(lead.work_type for lead in all_leads)),
        'value_range': {
            'min': min(lead.construction_value for lead in all_leads) if all_leads else 0,
            'max': max(lead.construction_value for lead in all_leads) if all_leads else 0
        }
    }
    
    return render_template('dashboard.html',
                         leads=filtered_leads,
                         total_leads=total_leads,
                         total_value=f"CAD ${total_value:,.2f}",
                         avg_value=f"CAD ${avg_value:,.2f}",
                         project_types=dict(project_types),
                         work_types=dict(work_types),
                         statuses=dict(statuses),
                         available_filters=available_filters,
                         request=request)

@app.route('/download-csv')
def download_csv():
    # Get filter parameters (same as dashboard)
    project_type_filter = request.args.get('project_type')
    location_filter = request.args.get('location')
    status_filter = request.args.get('status')
    work_type_filter = request.args.get('work_type')
    min_value = request.args.get('min_value', type=float)
    max_value = request.args.get('max_value', type=float)
    date_from = request.args.get('date_from')
    date_to = request.args.get('date_to')

    # Start with base query
    query = Lead.query

    # Apply filters
    if project_type_filter:
        query = query.filter_by(project_type=project_type_filter)
    if location_filter:
        query = query.filter_by(location=location_filter)
    if status_filter:
        query = query.filter_by(permit_status=status_filter)
    if work_type_filter:
        query = query.filter_by(work_type=work_type_filter)
    if min_value is not None:
        query = query.filter(Lead.construction_value >= min_value)
    if max_value is not None:
        query = query.filter(Lead.construction_value <= max_value)
    if date_from:
        query = query.filter(Lead.submission_date >= datetime.strptime(date_from, '%Y-%m-%d'))
    if date_to:
        query = query.filter(Lead.submission_date <= datetime.strptime(date_to, '%Y-%m-%d'))

    # Execute query
    filtered_leads = query.order_by(Lead.submission_date.desc()).all()

    # Create a StringIO object to write CSV data
    si = io.StringIO()
    writer = csv.writer(si)
    
    # Write headers
    headers = [
        'Permit Number',
        'Project Type',
        'Status',
        'Description',
        'Submission Date',
        'Issue Date',
        'Expiry Date',
        'Location',
        'Work Type',
        'Sub Work Type',
        'Construction Value',
        'Total Units',
        'Units Created',
        'Legal Description',
        'Latitude',
        'Longitude',
        'Owner',
        'Applicant',
        'Contractor',
        'Contractor Contact'
    ]
    writer.writerow(headers)
    
    # Write data rows
    for lead in filtered_leads:
        row = [
            lead.permit_number,
            lead.project_type,
            lead.permit_status,
            lead.permit_description,
            lead.submission_date.strftime('%Y-%m-%d %H:%M') if lead.submission_date else '',
            lead.issue_date.strftime('%Y-%m-%d %H:%M') if lead.issue_date else '',
            lead.expiry_date.strftime('%Y-%m-%d %H:%M') if lead.expiry_date else '',
            lead.location,
            lead.work_type,
            lead.sub_work_type,
            f"${lead.construction_value:,.2f}" if lead.construction_value else '',
            lead.total_units,
            lead.units_created,
            lead.legal_description,
            lead.latitude,
            lead.longitude,
            lead.owner,
            lead.applicant,
            lead.contractor,
            lead.contractor_contact
        ]
        writer.writerow(row)
    
    # Create the response
    output = si.getvalue()
    si.close()
    
    # Create a new StringIO object with the output
    mem = io.BytesIO()
    mem.write(output.encode('utf-8'))
    mem.seek(0)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    return send_file(
        mem,
        mimetype='text/csv',
        as_attachment=True,
        download_name=f'construction_leads_{timestamp}.csv'
    )

@app.route('/api/leads')
def api_leads():
    # Get filter parameters from query string
    project_type = request.args.get('project_type')
    location = request.args.get('location')
    
    # Start with base query
    query = Lead.query

    # Apply filters
    if project_type:
        query = query.filter_by(project_type=project_type)
    if location:
        query = query.filter_by(location=location)

    # Execute query
    filtered_leads = query.order_by(Lead.submission_date.desc()).all()
    
    # Format the response data
    response_data = [{
        'project_type': lead.project_type,
        'location': lead.location,
        'construction_value': lead.value_range,
        'permit_date': lead.submission_date.strftime('%Y-%m-%d %H:%M'),
    } for lead in filtered_leads]
    
    # Return JSON response with metadata
    return jsonify({
        'status': 'success',
        'total_results': len(response_data),
        'filters_applied': {
            'project_type': project_type,
            'location': location
        },
        'data': response_data
    })

@app.route('/api/leads/<int:lead_id>')
def get_lead_details(lead_id):
    lead = Lead.query.get_or_404(lead_id)
    return jsonify(lead.to_dict())

@app.route('/health')
def health_check():
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'template_dir': os.path.exists(template_dir),
        'static_dir': os.path.exists(static_dir),
        'database': 'connected' if db.engine.execute('SELECT 1').scalar() else 'error'
    })

if __name__ == '__main__':
    # Log startup information
    logger.info(f"Starting application in {os.environ.get('FLASK_ENV', 'production')} mode")
    logger.info(f"Template directory exists: {os.path.exists(template_dir)}")
    logger.info(f"Static directory exists: {os.path.exists(static_dir)}")
    
    # Bind to PORT if defined, otherwise default to 5000.
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)