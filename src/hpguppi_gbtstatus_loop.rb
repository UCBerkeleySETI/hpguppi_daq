#!/usr/bin/env ruby

require 'optparse'
require 'mysql'
require 'hashpipe'
require 'astroutil'

OPTS = {
  :create       => false,
  :delay        => 1.0,
  :instance_ids => (0..1),
  :foreground   => false
}

OP = OptionParser.new do |op|
  op.program_name = File.basename($0)

  op.banner = "Usage: #{op.program_name} [OPTIONS]"
  op.separator('')
  op.separator('Update hpguppi status buffer with GBT status.')
  op.separator('')
  op.separator('Options:')
  op.on('-c', '--[no-]create',
        "Create missing status buffers [#{OPTS[:create]}]") do |o|
    OPTS[:create] = o
  end
  op.on('-d', '--delay=SECONDS', Float,
        "Delay between updates (0.25-60) [#{OPTS[:delay]}]") do |o|
    o = 0.25 if o < 0.25
    o = 60.0 if o > 60.0
    OPTS[:delay] = o
  end
  op.on('-f', '--[no-]foreground',
        "Run in foreground [#{OPTS[:foreground]}]") do |o|
    OPTS[:foreground] = o
  end
  op.on('-i', '--instances=I[,...]', Array,
        "Instances to gateway [#{OPTS[:instance_ids]}]") do |o|
    OPTS[:instance_ids] = o.map {|s| Integer(s) rescue 0}
    OPTS[:instance_ids].uniq!
  end
  op.on_tail('-h','--help','Show this message') do
    puts op.help
    exit
  end
end
OP.parse!
#p OPTS; exit

# Become a daemon process unless running in foreground was requested
Process.daemon unless OPTS[:foreground]

FIELD_NAMES = [
  "scan_length",
  "remaining",
  "scan_sequence",
  "scan_number",
  "start_time",
  "time_to_start",
  "az_commanded",
  "el_commanded",
  "az_actual",
  "el_actual",
  "ant_motion",
  "time_to_target",
  "major_type",
  "minor_type",
  "major",
  "minor",
  "epoch",
  "receiver",
  "rcvr_pol",
  "freq",
  "if_rest_freq",
  "cal_state",
  "switch_period",
  "first_if_freq",
  "source",
  "observer",
  "last_update",
  "utc",
  "utc_date",
  "data_dir",
  "lst",
  "status",
  "time_to_set",
  "j2000_major",
  "j2000_minor"
]

SOL      = 299792458.0 # m/s
RADTODEG = 57.295779513082320876798154814105170332405472466564

#  beam_halfwidth(obs_freq, dish_diam=100.0)
#  Return the telescope beam halfwidth in arcmin
#  'obs_freq' = the observing frqeuency in MHz
#  'dish_diam' = the telescope diameter in m
def beam_halfwidth(obs_freq, dish_diam)
  1.2*SOL/(obs_freq*10.0**6)/dish_diam*RADTODEG*60/2
end

stats = OPTS[:instance_ids].map {|i| Hashpipe::Status.new(i, OPTS[:create])}

# TODO Add command line options for the mysql parameters
db = Mysql.new('gbtdata.gbt.nrao.edu', 'gbtstatus', 'w3bqu3ry', 'gbt_status')

while true
  g = db.query('select * from status limit 1').fetch_hash
  for stat in stats
    stat.lock do
      stat.hputs("TELESCOP", "GBT")
      stat.hputs("OBSERVER", g['observer']||'unknown')
      stat.hputs("PROJID", g['data_dir']||'unknown')
      stat.hputs("FRONTEND", g['receiver']||'unknown')
      stat.hputi4("NRCVR", 2) # I think all the GBT receivers have 2...
      stat.hputs("FD_POLN", g['rcvr_pol'] =~ /inear/ ? 'LIN' : 'CIRC')

      # Adjust OBSFREQ for bank number (assumes only 8 banks numbered 0 to 7)
      if g['receiver']=='Rcvr26_40'
        freq = Float(g['if_rest_freq'])
      else
        freq = Float(g['freq'])
      end
      banknum = stat.hgetr8('BANKNUM')  ||   3.5
      bankcnt = stat.hgeti4('BANKCNT')  ||   8
      obsbw   = stat.hgetr8('OBSBW')    || 187.5
      nchan   = stat.hgeti4('OBSNCHAN') ||  64   # nchan per port
      nchan  *= 8 # nchan per roach
      obsfreq = freq + obsbw * ((bankcnt-1.0)/2 - banknum) + 1500.0/nchan/2
      stat.hputr8("OBSFREQ", obsfreq)

      stat.hputs("SRC_NAME", g['source'])

      if g['ant_motion']=='Tracking' or g['ant_motion']=='Guiding'
        stat.hputs("TRK_MODE", 'TRACK')
      elsif g['ant_motion']=='Stopped'
        stat.hputs("TRK_MODE", 'DRIFT')
      else
        stat.hputs("TRK_MODE", 'UNKNOWN')
      end

      ra = Float(g['j2000_major'].split[0])
      ra_str = ra.to_hmsstr(4)
      dec = Float(g['j2000_minor'].split[0])
      if dec < 0
        dec_str = '-' + (-dec).to_hmsstr(4)
      else
        dec_str = '+' + dec.to_hmsstr(4)
      end
      stat.hputs("RA_STR", ra_str)
      stat.hputr8("RA", ra)
      stat.hputs("DEC_STR", dec_str)
      stat.hputr8("DEC", dec)
      h, m, s = g['lst'].split(":").map {|x| x.to_f}
      # Not sure why they convert to radians and then to seconds.
      # Also, not a fan of rounding fractional times forward into the future
      #lst = int(round(astro.hms_to_rad(int(h),int(m),float(s))*86400.0/astro.TWOPI))
      lst = (60*60*h + 60*m + s).to_i
      stat.hputr8("LST", lst)
      stat.hputr8("AZ", Float(g['az_actual']))
      stat.hputr8("ZA", 90.0-Float(g['el_actual']))
      beam_deg = 2.0*beam_halfwidth(freq, 100.0)/60.0
      stat.hputr8("BMAJ", beam_deg)
      stat.hputr8("BMIN", beam_deg)
    end # stat.lock
  end # for stat in stats

  sleep OPTS[:delay]
end
