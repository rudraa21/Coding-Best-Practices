# Databricks notebook source
padding = "{padding}"
border_color = "#CDCDCD"
font = "font-size:16px;"
weight = "font-weight:bold;"
align = "vertical-align:top;"
border = f"border-bottom:1px solid {border_color};"

def html_intro():
  return """<html><body><table style="width:100%">
            <p style="font-size:16px">The following variables and functions have been defined for you.<br/>Please refer to them in the following instructions.</p>"""

def html_header():
  return f"""<tr><th style="{border} padding: 0 1em 0 0; text-align:left">Variable/Function</th>
                 <th style="{border} padding: 0 1em 0 0; text-align:left">Description</th></tr>"""

def html_row_var(name, value, description):
  return f"""<tr><td style="{font} {weight} {padding} {align} white-space:nowrap; color:green;">{name}</td>
                 <td style="{font} {weight} {padding} {align} white-space:nowrap; color:blue;">{value}</td>
             </tr><tr><td style="{border} {font} {padding}">&nbsp;</td>
                 <td style="{border} {font} {padding} {align}">{description}</td></tr>"""

def html_row_fun(name, description):
  return f"""<tr><td style="{border}; {font} {padding} {align} {weight} white-space:nowrap; color:green;">{name}</td>
                 <td style="{border}; {font} {padding} {align}">{description}</td></td></tr>"""
