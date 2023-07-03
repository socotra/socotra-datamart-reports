import tkinter as tk
from tkinter import ttk

class PrimaryWindow(ttk.Frame):
    def __init__(self, master=None, reports=[]):
        ttk.Frame.__init__(self, master)               
        self.master = master
        self.master.title("Socotra Data Extract Generation")

        options = {'padx': 5, 'pady': 5}

        mylist = tk.Listbox(self)
        # for line in range(100):
        #   mylist.insert(END, "This is line number " + str(line))

        for report in reports:
          check = tk.Checkbutton(self, text=report, variable=1, onvalue=1, offvalue=0)
          mylist.insert(tk.END, check)
          check.pack()

        # label
        self.label = ttk.Label(self, text='Hello, Tkinter!')
        self.label.pack(**options)

        # button
        self.button = ttk.Button(self, text='Sync')
        self.button['command'] = self.button_clicked
        self.button.pack(**options)

        # show the frame on the container
        self.pack(**options)

    def button_clicked():
      print("You clicked it!")


class AppUI(tk.Tk):
    def __init__(self):
        super().__init__()
        # configure the root window
        self.title('My Awesome App')
        self.geometry('300x100')
