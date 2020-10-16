(defproject circledb "0.0.1"
  :description "Yoav Rubin's CircleDB"
  :url "https://github.com/sogaiu/circledb"
  :license {:name "MIT"
            :url  "https://github.com/aosabook/500lines/blob/master/LICENSE.md"}
  :source-paths ["src"]
  :dependencies [[org.clojure/clojure "1.10.1"]]
  ;; socket repl
  :jvm-opts ["-Dclojure.server.repl={:port 8238 :accept clojure.core.server/repl}"])
