on selectItem()
        -- 定义按钮
    set itemButtons to {"node51.ikh", "git pull", "other"}
    set temp to display dialog "爸爸，请选择您要的服务:" buttons itemButtons
    set itemName to button returned of temp
    if itemName is "node51.ikh" then
        runIkh()
    else if itemName is "git pull" then
        gitpull()
    else if itemName is "other" then
        set itemButtons to {"node51 test", "run phoenix"}
        set temp to display dialog "爸爸，请选择您要的服务:" buttons itemButtons
        set itemName to button returned of temp
        if itemName is "node51 test" then
            cdtest()
        else if itemName is "run phoenix" then
            runPhoenix()
        end if
    end if
end selectItem


on runIkh()
    set commandList to {"sh /data/dmp/run_impala.sh", "sh /data/dmp/hive/run_beeline_on_spark.sh", "sh /data/dmp/hive/run_beeline.sh"}
    set itemButtons to {"impala", "spark", "hive"}
    set temp to display dialog "爸爸，请选择计算引擎:" buttons itemButtons
    set itemName to button returned of temp
    if itemName is "impala" then
        set commandIndex to 1
    else if itemName is "spark" then
        set commandIndex to 2
    else if itemName is "hive" then
        set commandIndex to 3
    end if

    set commandStr to item commandIndex of commandList
    tell application "iTerm"
        tell current window
            set ikhtab to create tab with profile "node51.ikh.bigdata.dmp.com"
            tell first session of ikhtab
                -- root user 登录
                delay 1
                write text "1"
                delay 1
                write text "su impala"
                write text commandStr
                delay 2
                write text "use prod;"
            end tell
          end tell
    end tell
end runIkh


on gitpull()
    set itemButtons to {"node1", "node51"}
    set temp to display dialog "爸爸，请选择git pull代码的机器:" buttons itemButtons
    set itemName to button returned of temp
    if itemName is "node1" then
        set hostStr to "node1.azkaban.bigdata.dmp.com"
    else if itemName is "node51" then
        set hostStr to "node51.azkaban.bigdata.dmp.com"
    end if

    tell application "iTerm"
        tell current window
            set azkabantab to create tab with profile hostStr
            tell first session of azkabantab
                -- root user 登录
                delay 1
                write text "1"
                delay 1
                write text "su impala"
                write text "cd /data/dmp/warehouse-plus"
                write text "git pull"
                delay 1
                write text "fanrui"
                delay 1
                write text "fr112400"
                delay 5
                close
            end tell
          end tell
    end tell
end gitpull


on cdtest()
    tell application "iTerm"
        tell current window
            set azkabantab to create tab with profile "node51.azkaban.bigdata.dmp.com"
            tell first session of azkabantab
                -- root user 登录
                delay 1
                write text "1"
                delay 1
                write text "su impala"
                write text "cd /data/dmp/test/test/tmp"
            end tell
          end tell
    end tell
end cdtest


on runPhoenix()
    tell application "iTerm"
        tell current window
            set ikhtab to create tab with profile "node51.ikh.bigdata.dmp.com"
            tell first session of ikhtab
                -- root user 登录
                delay 1
                write text "1"
                delay 1
                write text "cd /data/dmp/phoenix/bin"
                write text "sh ./run_phoenix.sh"
            end tell
          end tell
    end tell
end runIkh


selectItem()


