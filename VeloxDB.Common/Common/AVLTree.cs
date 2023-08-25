using System;
using System.Collections.Generic;

namespace VeloxDB.Common;

internal sealed class AVLTree<TKey, TValue> : IEnumerable<KeyValuePair<TKey, TValue>>
{
	const int defaultCapacity = 16;

	// This is a special node with the sole purpose of holding a reference to the root so that the root node
	// would have a parent. This makes many operations on the root the same as on other nodes.
	// By convention we use left reference to hold the actual root od the tree.
	const int rootParent = 1;

	IComparer<TKey> comparer;

	int count;

	// We maintain tree nodes as structs inside an array and connect them using indexes.
	Node[] nodes;
	int usedCount;
	int freeList;

	// Since this collection returns items as a form of "pointers" to internal nodes it is
	// important to maintain version of the collection to be able to validate these pointers.
	int version;

	public AVLTree() :
		this(defaultCapacity, Comparer<TKey>.Default)
	{
	}

	public AVLTree(int capacity) :
		this(capacity, Comparer<TKey>.Default)
	{
	}

	public AVLTree(int capacity, IComparer<TKey> comparer)
	{
		this.comparer = comparer;
		nodes = new Node[capacity + 2];

		count = 0;
		usedCount = 2;  // Null node and root placeholder node

		freeList = 0;
		version = 0;
	}

	public int Count => count;
	public IComparer<TKey> Comparer => comparer;

	private int Root { get => nodes[rootParent].Left; set => nodes[rootParent].Left = value; }

	public void GetKeyValue(TreeItem item, out TKey key, out TValue value)
	{
		Node n = GetValidatedNode(item);
		key = n.Key;
		value = n.Value;
	}

	public TKey GetKey(TreeItem item)
	{
		Node n = GetValidatedNode(item);
		return n.Key;
	}

	public TValue GetValue(TreeItem item)
	{
		Node n = GetValidatedNode(item);
		return n.Value;
	}

	public TreeItem Find(TKey key)
	{
		if (count == 0)
			return new TreeItem();

		TreeItem item = FindLargerOrEqual(key, out bool equal);
		return equal ? item : new TreeItem(version, 0);
	}

	public TreeItem GetNext(TreeItem item)
	{
		ValidateNode(item);
		return new TreeItem(version, GetNextInternal(item.Index));
	}

	public TreeItem GetPrevious(TreeItem item)
	{
		ValidateNode(item);
		return new TreeItem(version, GetPrevInternal(item.Index));
	}

	public TreeItem GetSmallest()
	{
		return new TreeItem(version, FindSmallestInternal(Root));
	}

	public TreeItem GetLargest()
	{
		return new TreeItem(version, FindLargestInternal(Root));
	}

	public TreeItem FindSmaller(TKey key)
	{
		TreeItem item = FindSmallerOrEqual(key, out bool equal);
		if (equal)
			return new TreeItem(version, GetPrevInternal(item.Index));

		return item;
	}

	public TreeItem FindSmallerOrEqual(TKey key)
	{
		return FindSmallerOrEqual(key, out bool equal);
	}

	public TreeItem FindLarger(TKey key)
	{
		TreeItem item = FindLargerOrEqual(key, out bool equal);
		if (equal)
			return new TreeItem(version, GetNextInternal(item.Index));

		return item;
	}

	public TreeItem FindLargerOrEqual(TKey key)
	{
		return FindLargerOrEqual(key, out bool equal);
	}

	public void SetValue(TreeItem item, TValue value)
	{
		if (item.IsEmpty)
			ThrowEmptyItem();

		if (item.Version != version)
			ThrowCollectionModified();

		int node = item.Index;

		if (nodes[node].IsEmpty())
			ThrowInvalidItem();

		nodes[node].Value = value;
	}

	public void Add(TKey key, TValue value)
	{
		count++;
		version++;
		int node = NewNode();

		int parent = rootParent;
		int curr = Root;
		if (curr == 0)
		{
			nodes[node].Modify(key, value, rootParent, 0, 0, 1, 0);
			Root = node;
			return;
		}

		while (true)
		{
			int c = comparer.Compare(key, nodes[curr].Key);

			if (c > 0)
			{
				if (nodes[curr].Right == 0)
				{
					nodes[node].Modify(key, value, curr, 0, 0, 1, 0);
					nodes[curr].Right = node;
					nodes[curr].Height = 2;
					break;
				}
				else
				{
					parent = curr;
					curr = nodes[curr].Right;
				}
			}
			else if (c < 0)
			{
				if (nodes[curr].Left == 0)
				{
					nodes[node].Modify(key, value, curr, 0, 0, 1, 0);
					nodes[curr].Left = node;
					nodes[curr].Height = 2;
					break;
				}
				else
				{
					parent = curr;
					curr = nodes[curr].Left;
				}
			}
			else
			{
				nodes[node].Modify(key, value, 0, 0, 0, 0, nodes[curr].NextEqual);
				nodes[curr].NextEqual = node;
				return;
			}
		}

		curr = parent;
		RebalanceAncestors(curr);
	}

	public TreeItem Remove(TreeItem item)
	{
		if (item.IsEmpty)
			ThrowEmptyItem();

		if (item.Version != version)
			ThrowCollectionModified();

		if (nodes[item.Index].IsEmpty())
			ThrowInvalidItem();

		version++;
		count--;

		int nextNode = TryRemoveDuplicate(item.Index);
		if (nextNode != 0)
			return new TreeItem(version, nextNode);

		return new TreeItem(version, RemoveInternal(item.Index));
	}

	public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
	{
		int currVersion = version;

		int curr = FindSmallestInternal(Root);
		while (curr != 0)
		{
			int equalCurr = curr;
			while (equalCurr != 0)
			{
				yield return new KeyValuePair<TKey, TValue>(nodes[equalCurr].Key, nodes[equalCurr].Value);

				if (currVersion != version)
					ThrowCollectionModified();

				equalCurr = nodes[equalCurr].NextEqual;
			}

			curr = GetNextInternal(curr);
		}
	}

	public IEnumerable<TKey> Keys
	{
		get
		{
			int currVersion = version;

			int curr = FindSmallestInternal(Root);
			while (curr != 0)
			{
				int equalCurr = curr;
				while (equalCurr != 0)
				{
					yield return nodes[equalCurr].Key;
					if (currVersion != version)
						ThrowCollectionModified();

					equalCurr = nodes[equalCurr].NextEqual;
				}

				curr = GetNextInternal(curr);
			}
		}
	}

	public IEnumerable<TValue> Values
	{
		get
		{
			int currVersion = version;

			int curr = FindSmallestInternal(Root);
			while (curr != 0)
			{
				int equalCurr = curr;
				while (equalCurr != 0)
				{
					yield return nodes[equalCurr].Value;
					if (currVersion != version)
						ThrowCollectionModified();

					equalCurr = nodes[equalCurr].NextEqual;
				}

				curr = GetNextInternal(curr);
			}
		}
	}

	private TreeItem FindLargerOrEqual(TKey key, out bool equal)
	{
		equal = false;
		int curr = Root;
		int larger = 0;
		while (curr != 0)
		{
			int c = comparer.Compare(key, nodes[curr].Key);
			if (c > 0)
			{
				int rightChild = nodes[curr].Right;
				if (rightChild == 0)
					return new TreeItem(version, GetNextInternal(curr));

				curr = rightChild;
			}
			else if (c < 0)
			{
				larger = curr;
				int leftChild = nodes[curr].Left;
				if (leftChild == 0)
					return new TreeItem(version, curr);

				curr = leftChild;
			}
			else
			{
				equal = true;
				return new TreeItem(version, curr);
			}
		}

		return TreeItem.CreateEmpty();
	}

	private TreeItem FindSmallerOrEqual(TKey key, out bool equal)
	{
		equal = false;
		int curr = Root;
		while (curr != 0)
		{
			int c = comparer.Compare(key, nodes[curr].Key);
			if (c > 0)
			{
				int rightChild = nodes[curr].Right;
				if (rightChild == 0)
					return new TreeItem(version, curr);

				curr = rightChild;
			}
			else if (c < 0)
			{
				int leftChild = nodes[curr].Left;
				if (leftChild == 0)
					return new TreeItem(version, GetPrevInternal(curr));

				curr = leftChild;
			}
			else
			{
				equal = true;
				return new TreeItem(version, curr);
			}
		}

		return TreeItem.CreateEmpty();
	}

	private static void ThrowInvalidItem()
	{
		throw new ArgumentException("Tree item is invalid.");
	}

	private static void ThrowEmptyItem()
	{
		throw new ArgumentException("Tree item is empty.");
	}

	private static void ThrowCollectionModified()
	{
		throw new InvalidOperationException("Collection was modified after a tree item has been obtained.");
	}

	private int GetNextInternal(int node)
	{
		int rightChild = nodes[node].Right;
		int parent = nodes[node].Parent;

		if (rightChild == 0)
		{
			FindLeftParent(node, ref parent);
			if (parent == rootParent)
				return 0;

			return parent;
		}

		return FindSmallestInternal(rightChild);
	}

	private void FindLeftParent(int node, ref int parent)
	{
		int curr = node;
		while (parent != rootParent && nodes[parent].Right == curr)
		{
			curr = parent;
			parent = nodes[parent].Parent;
		}
	}

	private int GetPrevInternal(int node)
	{
		int leftChild = nodes[node].Left;
		int parent = nodes[node].Parent;

		if (leftChild == 0)
		{
			FindRightParent(node, ref parent);
			if (parent == rootParent)
				return 0;

			return parent;
		}

		return FindLargestInternal(leftChild);
	}

	private void FindRightParent(int node, ref int parent)
	{
		int curr = node;
		while (parent != rootParent && nodes[parent].Left == curr)
		{
			curr = parent;
			parent = nodes[parent].Parent;
		}
	}

	private void ValidateNode(TreeItem item)
	{
		if (item.IsEmpty)
			ThrowEmptyItem();

		if (item.Version != version)
			ThrowCollectionModified();

		if (nodes[item.Index].IsEmpty())
			ThrowInvalidItem();
	}

	private Node GetValidatedNode(TreeItem item)
	{
		if (item.IsEmpty)
			ThrowEmptyItem();

		if (item.Version != version)
			ThrowCollectionModified();

		Node n = nodes[item.Index];

		if (n.IsEmpty())
			ThrowInvalidItem();

		return n;
	}

	private int FindSmallestInternal(int node)
	{
		int curr = nodes[node].Left;
		while (curr != 0)
		{
			node = curr;
			curr = nodes[curr].Left;
		}

		return node;
	}

	private int FindLargestInternal(int node)
	{
		int curr = nodes[node].Right;
		while (curr != 0)
		{
			node = curr;
			curr = nodes[curr].Right;
		}

		return node;
	}

	private int TryRemoveDuplicate(int node)
	{
		int next = nodes[node].NextEqual;
		if (next == 0)
			return 0;

		Node removed = nodes[node];
		DeleteNode(node);

		if (removed.Left != 0)
			nodes[removed.Left].Parent = next;

		if (removed.Right != 0)
			nodes[removed.Right].Parent = next;

		nodes[next].Modify(removed.Parent, removed.Left, removed.Right, removed.Height);
		if (nodes[removed.Parent].Left == node)
		{
			nodes[removed.Parent].Left = next;
		}
		else
		{
			nodes[removed.Parent].Right = next;
		}

		return next;
	}

	private int RemoveInternal(int node)
	{
		int leftChild = nodes[node].Left;
		int rightChild = nodes[node].Right;
		int parent = nodes[node].Parent;
		if (leftChild == 0)
		{
			int next = GetNextInternal(node);
			ReplaceWithChild(node, rightChild, parent);
			RebalanceAncestors(parent);
			return next;
		}

		if (rightChild == 0)
		{
			int next = GetNextInternal(node);
			ReplaceWithChild(node, leftChild, parent);
			RebalanceAncestors(parent);
			return next;
		}

		int toRemove = FindSmallestInternal(rightChild);
		nodes[node].Key = nodes[toRemove].Key;
		nodes[node].Value = nodes[toRemove].Value;
		nodes[node].NextEqual = nodes[toRemove].NextEqual;
		RemoveInternal(toRemove);

		return node;
	}

	private void ReplaceWithChild(int node, int child, int parent)
	{
		DeleteNode(node);
		if (nodes[parent].Left == node)
			nodes[parent].Left = child;
		else
			nodes[parent].Right = child;

		if (child != 0)
			nodes[child].Parent = parent;
	}

	private int NewNode()
	{
		if (freeList == 0)
		{
			if (usedCount == nodes.Length)
				Array.Resize(ref nodes, nodes.Length * 2);

			return usedCount++;
		}

		int temp = freeList;
		freeList = nodes[freeList].NextEqual;
		return temp;
	}

	private void DeleteNode(int index)
	{
		nodes[index].Reset(freeList);
		freeList = index;
	}

	private void RebalanceAncestors(int node)
	{
		int parent = nodes[node].Parent;

		while (node != rootParent)
		{
			int prevHeight = nodes[node].Height;
			if (nodes[parent].Left == node)
				RebalanceLeft(parent, node);
			else
				RebalanceRight(parent, node);

			if (prevHeight == nodes[node].Height)
				break;

			node = parent;
			parent = nodes[parent].Parent;
		}
	}

	private void RebalanceRight(int parent, int node)
	{
		int leftNode = nodes[node].Left;
		int leftHeight = nodes[leftNode].Height;
		int leftNodeLeft = nodes[leftNode].Left;

		int rightNode = nodes[node].Right;
		int rightHeight = nodes[rightNode].Height;
		int rightNodeRight = nodes[rightNode].Right;

		int diff = rightHeight - leftHeight;
		if (diff == 2)
		{
			if (nodes[rightNodeRight].Height != rightHeight - 1)
				RightNodeRightRotate(node, rightNode);

			RightNodeLeftRotate(parent, node);
			return;
		}

		if (diff == -2)
		{
			if (nodes[leftNodeLeft].Height != leftHeight - 1)
				LeftNodeLeftRotate(node, leftNode);

			RightNodeRightRotate(parent, node);
			return;
		}

		nodes[node].Height = Math.Max(leftHeight, rightHeight) + 1;
	}

	private void RebalanceLeft(int parent, int node)
	{
		int leftNode = nodes[node].Left;
		int leftHeight = nodes[leftNode].Height;
		int leftNodeLeft = nodes[leftNode].Left;

		int rightNode = nodes[node].Right;
		int rightHeight = nodes[rightNode].Height;
		int rightNodeRight = nodes[rightNode].Right;

		int diff = rightHeight - leftHeight;
		if (diff == 2)
		{
			if (nodes[rightNodeRight].Height != rightHeight - 1)
				RightNodeRightRotate(node, rightNode);

			LeftNodeLeftRotate(parent, node);
			return;
		}

		if (diff == -2)
		{
			if (nodes[leftNodeLeft].Height != leftHeight - 1)
				LeftNodeLeftRotate(node, leftNode);

			LeftNodeRightRotate(parent, node);
			return;
		}

		nodes[node].Height = Math.Max(leftHeight, rightHeight) + 1;
	}

	private void LeftNodeRightRotate(int parent, int node)
	{
		int leftChild = nodes[node].Left;
		int leftChildRight = nodes[leftChild].Right;

		nodes[parent].Left = leftChild;

		int height = Math.Max(nodes[leftChildRight].Height, nodes[nodes[node].Right].Height) + 1;
		nodes[node].Parent = leftChild;
		nodes[node].Left = leftChildRight;
		nodes[node].Height = height;

		nodes[leftChild].Parent = parent;
		nodes[leftChild].Right = node;
		nodes[leftChild].Height = Math.Max(height, nodes[nodes[leftChild].Left].Height) + 1;

		if (leftChildRight != 0)
			nodes[leftChildRight].Parent = node;
	}

	private void LeftNodeLeftRotate(int parent, int node)
	{
		int rightChild = nodes[node].Right;
		int rightChildLeft = nodes[rightChild].Left;

		nodes[parent].Left = rightChild;

		int height = Math.Max(nodes[rightChildLeft].Height, nodes[nodes[node].Left].Height) + 1;
		nodes[node].Parent = rightChild;
		nodes[node].Right = rightChildLeft;
		nodes[node].Height = height;

		nodes[rightChild].Parent = parent;
		nodes[rightChild].Left = node;
		nodes[rightChild].Height = Math.Max(height, nodes[nodes[rightChild].Right].Height) + 1;

		if (rightChildLeft != 0)
			nodes[rightChildLeft].Parent = node;
	}

	private void RightNodeRightRotate(int parent, int node)
	{
		int leftChild = nodes[node].Left;
		int leftChildRight = nodes[leftChild].Right;

		nodes[parent].Right = leftChild;

		int height = Math.Max(nodes[leftChildRight].Height, nodes[nodes[node].Right].Height) + 1;
		nodes[node].Parent = leftChild;
		nodes[node].Left = leftChildRight;
		nodes[node].Height = height;

		nodes[leftChild].Parent = parent;
		nodes[leftChild].Right = node;
		nodes[leftChild].Height = Math.Max(height, nodes[nodes[leftChild].Left].Height) + 1;

		if (leftChildRight != 0)
			nodes[leftChildRight].Parent = node;
	}

	private void RightNodeLeftRotate(int parent, int node)
	{
		int rightChild = nodes[node].Right;
		int rightChildLeft = nodes[rightChild].Left;

		nodes[parent].Right = rightChild;

		int height = Math.Max(nodes[rightChildLeft].Height, nodes[nodes[node].Left].Height) + 1;
		nodes[node].Parent = rightChild;
		nodes[node].Right = rightChildLeft;
		nodes[node].Height = height;

		nodes[rightChild].Parent = parent;
		nodes[rightChild].Left = node;
		nodes[rightChild].Height = Math.Max(height, nodes[nodes[rightChild].Right].Height) + 1;

		if (rightChildLeft != 0)
			nodes[rightChildLeft].Parent = node;
	}

	System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
	{
		return GetEnumerator();
	}

	private struct Node
	{
		TKey key;
		TValue value;
		int left;
		int right;
		int parent;
		int height;
		int nextEqual;

		public TKey Key { get => key; set => key = value; }
		public TValue Value { get => value; set => this.value = value; }
		public int Left { get => left; set => left = value; }
		public int Right { get => right; set => right = value; }
		public int Parent { get => parent; set => parent = value; }
		public int Height { get => height; set => height = value; }
		public int NextEqual { get => nextEqual; set => nextEqual = value; }

		public void Modify(int parent, int left, int right, int height)
		{
			this.left = left;
			this.right = right;
			this.parent = parent;
			this.height = height;
		}

		public void Modify(TKey key, TValue value, int parent, int left, int right, int height, int next)
		{
			this.value = value;
			this.key = key;
			this.left = left;
			this.right = right;
			this.parent = parent;
			this.height = height;
			this.nextEqual = next;
		}

		public bool IsEmpty()
		{
			return parent == 0;
		}

		public void Reset(int freeList)
		{
			parent = 0;
			nextEqual = freeList;
		}
	}
}

internal struct TreeItem
{
	int version;
	int index;

	internal TreeItem(int version, int index)
	{
		this.version = version;
		this.index = index;
	}

	public bool IsEmpty => index == 0;

	internal int Version => version;
	internal int Index => index;

	public static TreeItem CreateEmpty()
	{
		return new TreeItem(0, 0);
	}
}
