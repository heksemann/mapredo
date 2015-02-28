
require "set"

module Mapredo
  @@parent = Hash.new
  @@child = Hash.new

  def initialize
    @parallel = nil
  end

  class Mapreducer
    attr_accessor :parallel,
                  :compression,
                  :files,
                  :buffer,
                  :workdir,
                  :subdir
    attr_reader :name, :map_only
    
    def initialize(name)
      @name = name
    end
    def input_file(filename)
      @input_file = filename
    end

    def input(mapreducer)
      Mapredo.add_child_requirement(self, mapreducer)
      Mapredo.add_parent_requirement(self, mapreducer)
    end

    def add_mapper(mapreducer)
      mapreducer.map_to_dir("somedir")
      @map_only = true
      @subdir = "somedir"
      @reduce_only = true
      Mapredo.add_parent_requirement(self, mapreducer)
    end

    def cmdline
      cmd = add_cmd
      if(@reduce_only)
        cmd += " && " + add_cmd(true)
      end
      cmd
    end

    def map_to_dir(directory)
      raise "Attempt to add #{} as mapper twice" if @map_only
      @map_only = true
      @subdir = directory
    end

    private

    def add_cmd(reduce_only = false)
      cmd = "mapredo "
      cmd += "-i '#{@input_file}' " if @input_file
      cmd += "-j #{@parallel} " if @parallel
      cmd += "--no-compression " if @compression == false
      cmd += "-b #{@buffer} " if @buffer
      cmd += "-f #{@files} " if @files
      if (reduce_only)
        cmd += "--reduce-only -s #{@subdir} "
      else
        cmd += "--map-only -s #{@subdir} " if @map_only
      end
      cmd += @name
    end
  end

  def Mapredo.make_mapreducer(name)
    mr = Mapreducer.new name
    @@child[mr] = Set.new
    @@parent[mr] = Set.new
    mr
  end

  def Mapredo.run
    if (@parallel)
      @@child.each do |mr,children|
        mr.parallel = @parallel
      end
    end

    cmdlines = Array.new
    done = Set.new

    while done.size < @@child.size
      @@child.each do |mr,children|
        if !done.include?(mr) && requirements_done(nil, mr, done)
          cmdlines.push build_cmdline(mr, children, done)
        end
      end
    end

    if done.empty?
      raise "Nothing to do"
    end

    cmdlines.each do |cmd|
      $stderr.puts cmd
      system cmd
      break if $? != 0
    end
  end

  def Mapredo.parallel(jobs)
    @parallel = jobs.to_int
    raise "Parallel must be a positive integer" if @parallel < 1
  end

  private

  def Mapredo.add_parent_requirement (to, from)
    @@parent[to].add from
  end

  def Mapredo.add_child_requirement (to, from)
    children = @@child[from]
    if (!children)
      raise "Unknown mapreducer"
    end
    children.add to
  end

  def Mapredo.build_cmdline(mapreducer, children, done)
    cmd = ""
    if children.empty?
      cmd = mapreducer.cmdline
    elsif children.size == 1
      cmd = "#{mapreducer.cmdline} | " \
            + "#{build_cmdline(children.first, @@child[children.first], done)}"
    else
      cmd = "#{mapreducer.cmdline} > tmpfile.txt"
      children.each do |mr|
        mr.input_file "tmpfile.txt"
      end
    end
    done.add mapreducer
    cmd
  end

  def Mapredo.requirements_done (parent, mapreducer, done)
    @@parent[mapreducer].each do |par|
      return nil if(par != parent && !done.member?(par))
    end

    children = @@child[mapreducer]
    if(children.size == 1)
      #return true if(children.first.map_only)
      return requirements_done(mapreducer, children.first, done)
    else
      return true
    end
  end

end
