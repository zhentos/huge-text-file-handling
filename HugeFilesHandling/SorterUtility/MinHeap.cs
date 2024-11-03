namespace SorterUtility
{
    // Binary Min-Heap implementation
    public class MinHeap
    {
        private List<(int Number, string Text, int FileIndex)> heap = new List<(int Number, string Text, int FileIndex)>();

        public void Insert((int Number, string Text, int FileIndex) item)
        {
            heap.Add(item);
            SiftUp(heap.Count - 1);
        }

        public (int Number, string Text, int FileIndex) ExtractMin()
        {
            if (heap.Count == 0) throw new InvalidOperationException("Heap is empty.");

            var minItem = heap[0];
            var lastItem = heap[heap.Count - 1];
            heap.RemoveAt(heap.Count - 1);

            if (heap.Count > 0)
            {
                heap[0] = lastItem;
                SiftDown(0);
            }

            return minItem;
        }

        public int Count => heap.Count;

        private void SiftUp(int index)
        {
            while (index > 0)
            {
                int parentIndex = (index - 1) / 2;
                if (heap[index].Number >= heap[parentIndex].Number) break;

                // Swap
                var temp = heap[index];
                heap[index] = heap[parentIndex];
                heap[parentIndex] = temp;

                index = parentIndex;
            }
        }

        private void SiftDown(int index)
        {
            int lastIndex = heap.Count - 1;
            while (index <= lastIndex)
            {
                int leftChildIndex = index * 2 + 1;
                int rightChildIndex = index * 2 + 2;
                int smallestIndex = index;

                if (leftChildIndex <= lastIndex && heap[leftChildIndex].Number < heap[smallestIndex].Number)
                    smallestIndex = leftChildIndex;

                if (rightChildIndex <= lastIndex && heap[rightChildIndex].Number < heap[smallestIndex].Number)
                    smallestIndex = rightChildIndex;

                if (smallestIndex == index) break;

                // Swap
                var temp = heap[index];
                heap[index] = heap[smallestIndex];
                heap[smallestIndex] = temp;

                index = smallestIndex;
            }
        }
    }
}
