app = Flask(__name__)

@app.route('/home', methods=['GET', 'POST'])
@login_required  # added @login_required
def home():

  if website_form.validate_on_submit():
  
      # execute scrapy spider
      execute_thread = threading.Thread(target=start_crawling, args=(website, merchant.id), daemon=True)
      execute_thread.start()
      
      return redirect(url_for('home'))
      
  return render_template('home.html', website_form=website_form, merchants=all_merchants)
        
        
