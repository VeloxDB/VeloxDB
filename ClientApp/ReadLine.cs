// The MIT License (MIT)

// Copyright(c) 2017 Toni Solarin-Sodara

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

using System;
using System.Text;

namespace Velox.ClientApp;

internal static class ReadLine
{
	private static List<string> _history;
	public static bool IsAlternateInput { get; set; } = false;
	public static bool IsRedirectedOrAlternate => Console.IsInputRedirected || IsAlternateInput;

	static ReadLine()
	{
		_history = new List<string>();
	}

	public static void AddHistory(params string[] text) => _history.AddRange(text);
	public static List<string> GetHistory() => _history;
	public static void ClearHistory() => _history = new List<string>();
	public static bool HistoryEnabled { get; set; }
	public static IAutoCompleteHandler AutoCompletionHandler { private get; set; }

	public static string Read(string @default = "")
	{
		KeyHandler keyHandler = new KeyHandler(new Console2(), _history, AutoCompletionHandler);
		string text = GetText(keyHandler);

		if (String.IsNullOrWhiteSpace(text) && !String.IsNullOrWhiteSpace(@default))
		{
			text = @default;
		}
		else
		{
			if (HistoryEnabled)
				_history.Add(text);
		}

		return text;
	}

	public static string ReadPassword(string prompt = "")
	{
		Console.Write(prompt);
		KeyHandler keyHandler = new KeyHandler(new Console2() { PasswordMode = true }, null, null);
		return GetText(keyHandler);
	}

	private static string GetText(KeyHandler keyHandler)
	{
		ConsoleKeyInfo keyInfo;
		if (Console.IsInputRedirected || IsAlternateInput)
		{
			keyInfo = ReadKeyRedirected();

			while (keyInfo.KeyChar != '\n')
			{
				if (keyInfo.Key != ConsoleKey.Enter)
					keyHandler.Handle(keyInfo);

				keyInfo = ReadKeyRedirected();
			}
		}
		else
		{
			keyInfo = Console.ReadKey(true);
			while (keyInfo.Key != ConsoleKey.Enter)
			{
				keyHandler.Handle(keyInfo);
				keyInfo = Console.ReadKey(true);
			}
		}

		Console.WriteLine();
		return keyHandler.Text;
	}

	private static ConsoleKeyInfo ReadKeyRedirected()
	{
		char ch = (char)Console.Read();
		if (ch == '\r')
			ch = (char)Console.Read();

		if (ch == '\n')
			return new ConsoleKeyInfo(ch, ConsoleKey.Enter, false, false, false);
		else
			return new ConsoleKeyInfo(ch, ConsoleKey.Oem8, false, false, false);
	}
}


internal class KeyHandler
{
	private int _cursorPos;
	private int _cursorLimit;
	private StringBuilder _text;
	private List<string> _history;
	private int _historyIndex;
	private ConsoleKeyInfo _keyInfo;
	private Dictionary<string, Action> _keyActions;
	private string[] _completions;
	private int _completionStart;
	private int _completionsIndex;
	private IConsole Console2;

	private bool IsStartOfLine() => _cursorPos == 0;

	private bool IsEndOfLine() => _cursorPos == _cursorLimit;

	private bool IsStartOfBuffer() => Console2.CursorLeft == 0;

	private bool IsEndOfBuffer() => Console2.CursorLeft == Console2.BufferWidth - 1;
	private bool IsInAutoCompleteMode() => _completions != null;

	private void MoveCursorLeft()
	{
		if (IsStartOfLine())
			return;

		if (IsStartOfBuffer())
			Console2.SetCursorPosition(Console2.BufferWidth - 1, Console2.CursorTop - 1);
		else
			Console2.SetCursorPosition(Console2.CursorLeft - 1, Console2.CursorTop);

		_cursorPos--;
	}

	private void MoveCursorHome()
	{
		while (!IsStartOfLine())
			MoveCursorLeft();
	}

	private string BuildKeyInput()
	{
		return (_keyInfo.Modifiers != ConsoleModifiers.Control && _keyInfo.Modifiers != ConsoleModifiers.Shift) ?
			_keyInfo.Key.ToString() : _keyInfo.Modifiers.ToString() + _keyInfo.Key.ToString();
	}

	private void MoveCursorRight()
	{
		if (IsEndOfLine())
			return;

		if (IsEndOfBuffer())
			Console2.SetCursorPosition(0, Console2.CursorTop + 1);
		else
			Console2.SetCursorPosition(Console2.CursorLeft + 1, Console2.CursorTop);

		_cursorPos++;
	}

	private void MoveCursorEnd()
	{
		while (!IsEndOfLine())
			MoveCursorRight();
	}

	private void ClearLine()
	{
		MoveCursorEnd();
		while (!IsStartOfLine())
			Backspace();
	}

	private void WriteNewString(string str)
	{
		ClearLine();
		foreach (char character in str)
			WriteChar(character);
	}

	private void WriteString(string str)
	{
		foreach (char character in str)
			WriteChar(character);
	}

	private void WriteChar() => WriteChar(_keyInfo.KeyChar);

	private void WriteChar(char c)
	{
		if (IsEndOfLine())
		{
			_text.Append(c);
			Console2.Write(c.ToString());
			_cursorPos++;
		}
		else
		{
			int left = Console2.CursorLeft;
			int top = Console2.CursorTop;
			string str = _text.ToString().Substring(_cursorPos);
			_text.Insert(_cursorPos, c);
			Console2.Write(c.ToString() + str);
			Console2.SetCursorPosition(left, top);
			MoveCursorRight();
		}

		_cursorLimit++;
	}

	private void Backspace()
	{
		if (IsStartOfLine())
			return;

		MoveCursorLeft();
		int index = _cursorPos;
		_text.Remove(index, 1);
		string replacement = _text.ToString().Substring(index);
		int left = Console2.CursorLeft;
		int top = Console2.CursorTop;
		Console2.Write(string.Format("{0} ", replacement));
		Console2.SetCursorPosition(left, top);
		_cursorLimit--;
	}

	private void Delete()
	{
		if (IsEndOfLine())
			return;

		int index = _cursorPos;
		_text.Remove(index, 1);
		string replacement = _text.ToString().Substring(index);
		int left = Console2.CursorLeft;
		int top = Console2.CursorTop;
		Console2.Write(string.Format("{0} ", replacement));
		Console2.SetCursorPosition(left, top);
		_cursorLimit--;
	}

	private void TransposeChars()
	{
		// local helper functions
		bool almostEndOfLine() => (_cursorLimit - _cursorPos) == 1;
		int incrementIf(Func<bool> expression, int index) => expression() ? index + 1 : index;
		int decrementIf(Func<bool> expression, int index) => expression() ? index - 1 : index;

		if (IsStartOfLine()) { return; }

		var firstIdx = decrementIf(IsEndOfLine, _cursorPos - 1);
		var secondIdx = decrementIf(IsEndOfLine, _cursorPos);

		var secondChar = _text[secondIdx];
		_text[secondIdx] = _text[firstIdx];
		_text[firstIdx] = secondChar;

		var left = incrementIf(almostEndOfLine, Console2.CursorLeft);
		var cursorPosition = incrementIf(almostEndOfLine, _cursorPos);

		WriteNewString(_text.ToString());

		Console2.SetCursorPosition(left, Console2.CursorTop);
		_cursorPos = cursorPosition;

		MoveCursorRight();
	}

	private void StartAutoComplete()
	{
		while (_cursorPos > _completionStart)
			Backspace();

		_completionsIndex = 0;

		WriteString(_completions[_completionsIndex]);
	}

	private void NextAutoComplete()
	{
		while (_cursorPos > _completionStart)
			Backspace();

		_completionsIndex++;

		if (_completionsIndex == _completions.Length)
			_completionsIndex = 0;

		WriteString(_completions[_completionsIndex]);
	}

	private void PreviousAutoComplete()
	{
		while (_cursorPos > _completionStart)
			Backspace();

		_completionsIndex--;

		if (_completionsIndex == -1)
			_completionsIndex = _completions.Length - 1;

		WriteString(_completions[_completionsIndex]);
	}

	private void PrevHistory()
	{
		if (_historyIndex > 0)
		{
			_historyIndex--;
			WriteNewString(_history[_historyIndex]);
		}
	}

	private void NextHistory()
	{
		if (_historyIndex < _history.Count)
		{
			_historyIndex++;
			if (_historyIndex == _history.Count)
				ClearLine();
			else
				WriteNewString(_history[_historyIndex]);
		}
	}

	private void ResetAutoComplete()
	{
		_completions = null;
		_completionsIndex = 0;
	}

	public string Text
	{
		get
		{
			return _text.ToString();
		}
	}

	public KeyHandler(IConsole console, List<string> history, IAutoCompleteHandler autoCompleteHandler)
	{
		Console2 = console;

		_history = history ?? new List<string>();
		_historyIndex = _history.Count;
		_text = new StringBuilder();
		_keyActions = new Dictionary<string, Action>();

		_keyActions["LeftArrow"] = MoveCursorLeft;
		_keyActions["Home"] = MoveCursorHome;
		_keyActions["End"] = MoveCursorEnd;
		_keyActions["ControlA"] = MoveCursorHome;
		_keyActions["ControlB"] = MoveCursorLeft;
		_keyActions["RightArrow"] = MoveCursorRight;
		_keyActions["ControlF"] = MoveCursorRight;
		_keyActions["ControlE"] = MoveCursorEnd;
		_keyActions["Backspace"] = Backspace;
		_keyActions["Delete"] = Delete;
		_keyActions["ControlD"] = Delete;
		_keyActions["ControlH"] = Backspace;
		_keyActions["ControlL"] = ClearLine;
		_keyActions["Escape"] = ClearLine;
		_keyActions["UpArrow"] = PrevHistory;
		_keyActions["ControlP"] = PrevHistory;
		_keyActions["DownArrow"] = NextHistory;
		_keyActions["ControlN"] = NextHistory;
		_keyActions["ControlU"] = () =>
		{
			while (!IsStartOfLine())
				Backspace();
		};
		_keyActions["ControlK"] = () =>
		{
			int pos = _cursorPos;
			MoveCursorEnd();
			while (_cursorPos > pos)
				Backspace();
		};
		_keyActions["ControlW"] = () =>
		{
			while (!IsStartOfLine() && _text[_cursorPos - 1] != ' ')
				Backspace();
		};
		_keyActions["ControlT"] = TransposeChars;

		_keyActions["Tab"] = () =>
		{
			if (IsInAutoCompleteMode())
			{
				NextAutoComplete();
			}
			else
			{
				if (autoCompleteHandler == null || !IsEndOfLine())
					return;

				string text = _text.ToString();

				_completionStart = text.LastIndexOfAny(autoCompleteHandler.Separators);
				_completionStart = _completionStart == -1 ? 0 : _completionStart + 1;

				_completions = autoCompleteHandler.GetSuggestions(text, _completionStart);
				_completions = _completions?.Length == 0 ? null : _completions;

				if (_completions == null)
					return;

				StartAutoComplete();
			}
		};

		_keyActions["ShiftTab"] = () =>
		{
			if (IsInAutoCompleteMode())
			{
				PreviousAutoComplete();
			}
		};
	}

	public void Handle(ConsoleKeyInfo keyInfo)
	{
		_keyInfo = keyInfo;

		// If in auto complete mode and Tab wasn't pressed
		if (IsInAutoCompleteMode() && _keyInfo.Key != ConsoleKey.Tab)
			ResetAutoComplete();

		Action action;
		_keyActions.TryGetValue(BuildKeyInput(), out action);
		action = action ?? WriteChar;
		action.Invoke();
	}
}

internal class Console2 : IConsole
{
	public int CursorLeft => Console.CursorLeft;

	public int CursorTop => Console.CursorTop;

	public int BufferWidth => Console.BufferWidth;

	public int BufferHeight => Console.BufferHeight;

	public bool PasswordMode { get; set; }

	public void SetCursorPosition(int left, int top)
	{
		if (!PasswordMode)
			Console.SetCursorPosition(left, top);
	}

	public void Write(string value)
	{
		if (PasswordMode)
			value = new String(default(char), value.Length);

		Console.Write(value);
	}

	public void WriteLine(string value) => Console.WriteLine(value);
}

internal interface IConsole
{
	int CursorLeft { get; }
	int CursorTop { get; }
	int BufferWidth { get; }
	int BufferHeight { get; }
	void SetCursorPosition(int left, int top);
	void Write(string value);
	void WriteLine(string value);
}

public interface IAutoCompleteHandler
{
	char[] Separators { get; set; }
	string[] GetSuggestions(string text, int index);
}
