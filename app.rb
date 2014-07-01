
require 'zillabyte' 
require 'zlib'
require 'anemone'

MAX_CRAWL_PAGES = 500
EMIT_TIMEOUT = 15

app = Zillabyte.app("deep_crawler")

stream = app.source do

  prepare do 
    @domains = []
    gz = Zlib::GzipReader.open('domains.gz')
    gz.each_line do |line|
      @domains << line
    end
    @domains.shuffle!
  end
  
  next_tuple do 
    begin
      if @domains.size > 0
        emit :url => @domains.shift.strip
      end
    rescue Exception => e
      log "error (source): #{e}"
    end
  end
  
end


stream = stream.each do  
  execute do |tuple|
    
    # Normalize the url
    begin 
    
      # Init 
      url = tuple['url']
      log "crawling domain: #{url}"
    
      $last_emitted = Time.now
      thread = Thread.new do 

        base_url = URI.parse("http://#{url.gsub(/^http(s)?:\/\//,"")}")
        max_crawl = MAX_CRAWL_PAGES
        pages_left = max_crawl
        visited = {}
      
        Anemone.crawl(base_url, :read_timeout => 4, :skip_query_strings => true, :obey_robots_txt => true) do |anemone|
  
          anemone.on_every_page do |page|
            url = page.url
            unless visited[page.url.to_s.gsub(/\/$/,'')] 
              if page.redirect?
              
              else
                
                body = page.body
                if body.include?("<html")
                  $last_emitted = Time.now
                  visited[page.url.to_s.gsub(/\/$/,'')] = true
                  emit(:url => url.to_s, :html => body, :domain => url.host.to_s)  
                end
              end
            end
          end
  
          anemone.focus_crawl do |page|
    
            if page.redirect_to
              [page.redirect_to]
            
            elsif (pages_left > 0)
    
              # Select only the links on this domain... 
              same_host_links = page.links.select do |url|
                url.host.ends_with?(base_url.host)
              end
      
              # Remove stuff we've already seen... 
              same_host_links.select! do |url|
                !visited[url.to_s.gsub(/\/$/,'')]
              end
      
              # Unique'ify
              same_host_links.uniq!
      
              # Order by size
              same_host_links.sort! do |a, b|
                a.to_s.size <=> b.to_s.size
              end
    
              # Offer urls up.. 
              next_links = same_host_links.first(pages_left)
              pages_left -= next_links.size
    
              # Done..
              next_links
      
            else
              # Done. no links.. 
              []
            end
    
          end
          
    
        end
      end
      
      # Abort if nothing gets emitted for a while... 
      while(true)
        if $last_emitted + EMIT_TIMEOUT < Time.now
          log "killing thread because of timeout: #{url.to_s}"
          thread.kill()
        end
        break unless thread.alive?
        sleep 1
      end
      
    rescue Exception => e
      log "error: #{e}"
    end
    
  end  
end


stream.sink do
  name "web_deep"
  column "url", :string
  column "domain", :string
  column "html", :string
end


