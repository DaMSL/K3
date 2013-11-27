K3 MapReduce implementation
===========================

Description
-----------

Distributed algorithms implementation based mapReduce

emacs K3-mode
-------------

1. Download k3-mode.el 
2. put it into your .emacs.d
3. append following lines into your .emacs

(load-file "/path/to/.emacs.d/k3-mode.el")

(provide 'k3-mode)

(add-to-list 'auto-mode-alist '("\\.k3\\'" . k3-mode))

4. restart emacs
